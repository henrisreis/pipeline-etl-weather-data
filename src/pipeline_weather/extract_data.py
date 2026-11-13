import requests
import json
from pathlib import Path

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_weather_data(url: str, params: dict) -> list:
    response = requests.get(url, params=params)
    data = response.json()

    if response.status_code != 200:
        logging.error("Erro na requisição!")
        return []

    if not data:
        logging.warning("Nenhum dado retornado!")
        return []

    output_path = "data/weather_data.json"
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)

    logging.info(f"\nArquivo salvo em {output_path}")
    return data
