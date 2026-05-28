"""
Cliente robusto para la API externa de datos (RF1 / sección 6.1).
Imagen: cristiandiaz13/mlops-puj:data-api-pf-v1
  docker run -d --name data-api -p 8000:80 cristiandiaz13/mlops-puj:data-api-pf-v1

Contrato confirmado (Fase 0):
  GET /health                              -> {"status": "OK"}
  GET /data?group_number=N                 -> {"group_number", "batch_number", "data": [...]}
       · cursor con estado en el servidor: batch_number se autoincrementa en cada llamada
       · HTTP 400 {"detail": "Ya se recolectó toda la información mínima necesaria"} = fin de datos
  GET /restart_data_generation?group_number=N -> {"ok": true}  (reinicia el cursor a 0)

Cada registro trae 12 columnas:
  brokered_by, status, price, bed, bath, acre_lot, street, city, state,
  zip_code, house_size, prev_sold_date
(brokered_by/street/zip_code llegan como float; status/city/state/prev_sold_date como str).
"""

import logging
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Columnas esperadas del dataset (para validate_schema)
EXPECTED_COLUMNS = [
    "brokered_by", "status", "price", "bed", "bath", "acre_lot",
    "street", "city", "state", "zip_code", "house_size", "prev_sold_date",
]


class DataAPIError(Exception):
    """Error irrecuperable consumiendo la API de datos."""


class NoMoreDataError(DataAPIError):
    """La API ya no entrega más lotes (HTTP 400 = fin de datos disponibles)."""


def _build_session(total_retries: int = 3, backoff: float = 1.0) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],   # 400 NO se reintenta: es fin de datos
        allowed_methods=["GET"],
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


class DataAPIClient:
    def __init__(self, base_url: str, group_number: int = 1, timeout: int = 120):
        self.base_url = base_url.rstrip("/")
        self.group_number = group_number
        self.timeout = timeout
        self.session = _build_session()

    def health(self) -> bool:
        try:
            r = self.session.get(f"{self.base_url}/health", timeout=10)
            return r.ok and r.json().get("status") == "OK"
        except requests.RequestException:
            return False

    def fetch_batch(self, group_number: Optional[int] = None) -> dict:
        """Solicita el SIGUIENTE lote disponible (el servidor avanza el cursor).
        Returns: {"group_number", "batch_number", "data": [...registros...]}.
        Raises:  NoMoreDataError (HTTP 400) si no quedan lotes;
                 DataAPIError en cualquier otro fallo definitivo.
        """
        group = group_number if group_number is not None else self.group_number
        url = f"{self.base_url}/data"
        try:
            resp = self.session.get(url, params={"group_number": group}, timeout=self.timeout)
        except requests.RequestException as e:
            raise DataAPIError(f"Fallo de conexión con la API de datos: {e}") from e

        if resp.status_code == 400:
            raise NoMoreDataError(f"Fin de datos para el grupo {group}: {resp.text}")
        if not resp.ok:
            raise DataAPIError(f"HTTP {resp.status_code}: {resp.text}")

        payload = resp.json()
        records = payload.get("data") or []
        if not records:
            raise NoMoreDataError("Respuesta vacía: no hay registros en el lote")
        logger.info("Lote %s (grupo %s): %s registros",
                    payload.get("batch_number"), group, len(records))
        return payload

    def restart(self, group_number: Optional[int] = None) -> bool:
        """Reinicia el cursor del grupo a batch_number=0 (reprocesamiento controlado)."""
        group = group_number if group_number is not None else self.group_number
        r = self.session.get(f"{self.base_url}/restart_data_generation",
                             params={"group_number": group}, timeout=self.timeout)
        return r.ok and r.json().get("ok", False)
