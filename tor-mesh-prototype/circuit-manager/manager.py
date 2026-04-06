#!/usr/bin/env python3

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, jsonify, request
import stem
import stem.control
from stem import Signal
from stem.control import Controller

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [circuit-mgr] %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

logging.getLogger("stem").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)


@dataclass
class NodeConfig:
    """Параметры одной Tor-ноды."""

    node_id: str
    host: str
    control_port: int
    password: str


@dataclass
class NodeState:
    """
    Текущее состояние ноды.
    """

    config: NodeConfig
    controller: Optional[Controller] = None
    is_connected: bool = False
    is_bootstrapped: bool = False
    last_rotation_at: float = 0.0
    rotation_count: int = 0
    failed_connections: int = 0
    # Количество активных цепей из GETINFO
    circuit_count: int = 0
    last_seen_at: float = field(default_factory=time.time)

    def age_since_rotation(self) -> float:
        return time.time() - self.last_rotation_at

    def to_dict(self) -> dict:
        return {
            "node_id": self.config.node_id,
            "host": self.config.host,
            "control_port": self.config.control_port,
            "connected": self.is_connected,
            "bootstrapped": self.is_bootstrapped,
            "rotation_count": self.rotation_count,
            "last_rotation_ago_sec": round(self.age_since_rotation()),
            "circuit_count": self.circuit_count,
            "failed_connections": self.failed_connections,
        }


def _load_node_configs() -> list[NodeConfig]:
    """Загружает конфиги нод из переменных окружения."""
    password = os.environ.get("TOR_CONTROL_PASSWORD", "changeme")
    return [
        NodeConfig(
            node_id="tor1",
            host=os.environ.get("TOR1_HOST", "172.28.0.10"),
            control_port=int(os.environ.get("TOR1_CONTROL_PORT", 9051)),
            password=password,
        ),
        NodeConfig(
            node_id="tor2",
            host=os.environ.get("TOR2_HOST", "172.28.0.11"),
            control_port=int(os.environ.get("TOR2_CONTROL_PORT", 9053)),
            password=password,
        ),
    ]


class CircuitManager:
    _NEWNYM_COOLDOWN = 10

    def __init__(self):
        self._lock = threading.RLock()

        configs = _load_node_configs()
        self._nodes: dict[str, NodeState] = {
            cfg.node_id: NodeState(config=cfg) for cfg in configs
        }

        self._rotation_interval = int(os.environ.get("CIRCUIT_ROTATION_INTERVAL", 300))
        self._max_circuit_age = int(os.environ.get("MAX_CIRCUIT_AGE", 900))
        self._min_circuits = int(os.environ.get("MIN_CIRCUITS", 2))

        self._start_time = time.time()
        self._total_rotations = 0

        logger.info(
            "CircuitManager init | nodes=%s | rotation_interval=%ds | "
            "max_circuit_age=%ds | min_circuits=%d",
            list(self._nodes.keys()),
            self._rotation_interval,
            self._max_circuit_age,
            self._min_circuits,
        )

    def connect(self, node_id: str) -> bool:
        """
        Подключается (или переподключается) к Control Port ноды.
        Возвращает True при успехе.
        """
        with self._lock:
            state = self._nodes.get(node_id)
            if state is None:
                logger.error("Unknown node: %s", node_id)
                return False

            cfg = state.config

            # Закрываем старый контроллер если есть
            self._safe_close(state)

            try:
                ctrl = Controller.from_port(
                    address=cfg.host,
                    port=cfg.control_port,
                )
                ctrl.authenticate(password=cfg.password)

                # Проверяем bootstrap
                bootstrap_phase = ctrl.get_info("status/bootstrap-phase", default="")
                bootstrapped = "PROGRESS=100" in bootstrap_phase

                state.controller = ctrl
                state.is_connected = True
                state.is_bootstrapped = bootstrapped
                state.failed_connections = 0
                state.last_seen_at = time.time()

                version = ctrl.get_version()
                logger.info(
                    "Connected to %s @ %s:%d | Tor %s | bootstrap=%s",
                    node_id,
                    cfg.host,
                    cfg.control_port,
                    version,
                    bootstrapped,
                )
                return True

            except Exception as exc:
                state.is_connected = False
                state.is_bootstrapped = False
                state.failed_connections += 1
                logger.warning(
                    "Cannot connect to %s (%s:%d): %s [attempt #%d]",
                    node_id,
                    cfg.host,
                    cfg.control_port,
                    exc,
                    state.failed_connections,
                )
                return False

    def connect_all(self):
        """Подключается ко всем нодам при старте."""
        for node_id in self._nodes:
            self.connect(node_id)

    def rotate(self, node_id: Optional[str] = None) -> dict[str, bool]:
        """
        Отправляет NEWNYM конкретной ноде или всем здоровым нодам.
        Возвращает словарь {node_id: success}.
        """
        targets = (
            [node_id]
            if node_id
            else [nid for nid, s in self._nodes.items() if s.is_connected]
        )
        results: dict[str, bool] = {}

        for nid in targets:
            results[nid] = self._rotate_one(nid)

        return results

    def _rotate_one(self, node_id: str) -> bool:
        """Ротирует одну ноду"""
        with self._lock:
            state = self._nodes.get(node_id)
            if state is None:
                return False

            # КД
            elapsed = time.time() - state.last_rotation_at
            if elapsed < self._NEWNYM_COOLDOWN:
                logger.debug(
                    "Skipping rotation for %s: cooldown %.1fs remaining",
                    node_id,
                    self._NEWNYM_COOLDOWN - elapsed,
                )
                return False

            if not state.is_connected or state.controller is None:
                logger.warning("Cannot rotate %s: not connected", node_id)
                self.connect(node_id)
                return False

            try:
                state.controller.signal(Signal.NEWNYM)
                state.last_rotation_at = time.time()
                state.rotation_count += 1
                self._total_rotations += 1
                logger.info(
                    "NEWNYM sent to %s | total rotations: %d",
                    node_id,
                    self._total_rotations,
                )
                return True

            except stem.SocketClosed:
                logger.warning(
                    "Socket closed on %s during rotation, reconnecting...", node_id
                )
                state.is_connected = False
                self.connect(node_id)
                return False

            except Exception as exc:
                logger.error("Rotation failed for %s: %s", node_id, exc)
                state.is_connected = False
                return False

    def scheduled_rotate_all(self):
        """APScheduler job: плановая ротация всех нод."""
        logger.info("Scheduled circuit rotation...")
        results = self.rotate()
        logger.info("Rotation results: %s", results)

    def health_check_all(self):
        """
        APScheduler job: проверяем состояние нод.
        При падении — переподключаемся.
        При малом количестве цепей — внеплановая ротация.
        """
        with self._lock:
            for node_id, state in self._nodes.items():
                self._check_node(node_id, state)

    def _check_node(self, node_id: str, state: NodeState):
        """Проверяет одну ноду, вызывается из health_check_all."""
        if state.controller is None or not state.is_connected:
            logger.info("Node %s is disconnected, trying to reconnect...", node_id)
            self.connect(node_id)
            return

        try:
            bootstrap = state.controller.get_info("status/bootstrap-phase", default="")
            state.is_bootstrapped = "PROGRESS=100" in bootstrap
            state.last_seen_at = time.time()

            # Считаем активные цепи
            circuits = state.controller.get_circuits()
            built = [c for c in circuits if c.status == stem.CircStatus.BUILT]
            state.circuit_count = len(built)

            logger.debug(
                "Node %s OK | bootstrapped=%s | circuits=%d",
                node_id,
                state.is_bootstrapped,
                state.circuit_count,
            )

            # Если цепей меньше минимума — ротируем
            if state.circuit_count < self._min_circuits:
                logger.warning(
                    "Node %s: low circuit count (%d < %d), forcing rotation",
                    node_id,
                    state.circuit_count,
                    self._min_circuits,
                )
                self._rotate_one(node_id)

        except stem.SocketClosed:
            logger.warning("Node %s: socket closed, reconnecting...", node_id)
            state.is_connected = False
            state.is_bootstrapped = False
            self.connect(node_id)

        except Exception as exc:
            logger.error("Health check error on %s: %s", node_id, exc)
            state.is_connected = False

    def healthy_nodes(self) -> list[str]:
        """Список node_id нод, которые подключены и прошли bootstrap."""
        with self._lock:
            return [
                nid
                for nid, s in self._nodes.items()
                if s.is_connected and s.is_bootstrapped
            ]

    def get_status(self) -> dict:
        """Полный статус для /api/status."""
        with self._lock:
            return {
                "uptime_sec": round(time.time() - self._start_time),
                "total_rotations": self._total_rotations,
                "rotation_interval_sec": self._rotation_interval,
                "healthy_nodes": self.healthy_nodes(),
                "nodes": {nid: state.to_dict() for nid, state in self._nodes.items()},
            }

    @staticmethod
    def _safe_close(state: NodeState):
        """Закрывает контроллер без исключений."""
        if state.controller is not None:
            try:
                state.controller.close()
            except Exception:
                pass
            state.controller = None
            state.is_connected = False
            state.is_bootstrapped = False

    def shutdown(self):
        """Чистое завершение — закрываем все контроллеры."""
        logger.info("Shutting down CircuitManager...")
        with self._lock:
            for state in self._nodes.values():
                self._safe_close(state)


def create_app(manager: CircuitManager) -> Flask:
    """
    Эндпоинты:
        GET /health          — liveness probe (используется docker healthcheck)
        GET /api/status      — полный статус нод и цепей
        POST /api/rotate     — немедленная ротация (всех или одной ноды)
    """
    app = Flask(__name__)

    @app.get("/health")
    def health():
        """
        Liveness + readiness probe.
        503 если нет ни одной здоровой ноды.
        """
        healthy = manager.healthy_nodes()
        ok = len(healthy) > 0
        return jsonify(
            {
                "status": "ok" if ok else "degraded",
                "healthy_nodes": healthy,
            }
        ), (HTTPStatus.OK if ok else HTTPStatus.SERVICE_UNAVAILABLE)

    @app.get("/api/status")
    def status():
        return jsonify(manager.get_status())

    @app.post("/api/rotate")
    def rotate():
        """
        Тело запроса (опционально):
            {"node_id": "tor1"}   — ротировать конкретную ноду
            {}                     — ротировать все здоровые ноды
        """
        body = request.get_json(silent=True) or {}
        node_id = body.get("node_id")
        results = manager.rotate(node_id)
        success = any(results.values())
        return jsonify(
            {
                "rotated": results,
                "success": success,
            }
        ), (HTTPStatus.OK if success else HTTPStatus.SERVICE_UNAVAILABLE)

    return app


def create_scheduler(manager: CircuitManager) -> BackgroundScheduler:
    """
    Настраивает APScheduler с двумя job-ами:
      • rotation_job  — плановая ротация цепей
      • health_job    — проверка здоровья нод
    """
    scheduler = BackgroundScheduler(
        timezone="UTC",
        job_defaults={"coalesce": True, "max_instances": 1},
    )

    rotation_interval = int(os.environ.get("CIRCUIT_ROTATION_INTERVAL", 300))
    scheduler.add_job(
        manager.scheduled_rotate_all,
        trigger=IntervalTrigger(seconds=rotation_interval),
        id="rotation_job",
        name="Tor circuit rotation",
        next_run_time=None,  # не запускать сразу при старте
    )

    scheduler.add_job(
        manager.health_check_all,
        trigger=IntervalTrigger(seconds=30),
        id="health_job",
        name="Node health check",
    )

    return scheduler


def main():
    manager = CircuitManager()

    # Подключаемся ко всем нодам при старте
    manager.connect_all()

    # APScheduler (фоновые потоки)
    scheduler = create_scheduler(manager)
    scheduler.start()
    logger.info("Scheduler started")

    # Flask (основной поток)
    app = create_app(manager)
    logger.info("HTTP API listening on :9999")

    try:
        app.run(host="0.0.0.0", port=9999, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.shutdown(wait=False)
        manager.shutdown()
        logger.info("Stopped")


if __name__ == "__main__":
    main()
