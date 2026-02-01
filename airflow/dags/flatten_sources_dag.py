from datetime import datetime
import json
import xml.etree.ElementTree as ET
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/data"

def flatten_pets_json():
    with open(f"{DATA_DIR}/pets-data.json", "r", encoding="utf-8") as f:
        raw = json.load(f)

    pets = raw.get("pets", [])
    records = []

    for pet in pets:
        base = {
            "name": pet.get("name", "").strip(),
            "species": pet.get("species", "").strip(),
            "birthYear": pet.get("birthYear"),
            "photo": pet.get("photo", "").strip()
        }
        fav_foods = pet.get("favFoods", [])
        if not fav_foods:
            records.append({**base, "favFood": None})
        else:
            for food in fav_foods:
                records.append({**base, "favFood": food.strip()})

    df = pd.DataFrame(records)
    df.to_csv(f"{OUTPUT_DIR}/flattened_pets.csv", index=False)


def flatten_nutrition_xml():
    tree = ET.parse(f"{DATA_DIR}/nutrition.xml")
    root = tree.getroot()

    foods = []
    for food in root.findall("food"):
        record = {}
        record["name"] = food.find("name").text.strip() if food.find("name") is not None else ""
        record["mfr"] = food.find("mfr").text.strip() if food.find("mfr") is not None else ""
        serving_elem = food.find("serving")
        record["serving"] = serving_elem.text.strip() if serving_elem is not None else ""
        record["serving_units"] = serving_elem.get("units", "") if serving_elem is not None else ""

        calories = food.find("calories")
        record["calories_total"] = calories.get("total", "") if calories is not None else ""
        record["calories_fat"] = calories.get("fat", "") if calories is not None else ""

        nutrients = ["total-fat", "saturated-fat", "cholesterol", "sodium", "carb", "fiber", "protein"]
        for n in nutrients:
            elem = food.find(n)
            record[n.replace("-", "_")] = elem.text.strip() if elem is not None else ""

        vitamins = food.find("vitamins")
        if vitamins is not None:
            for v in ["a", "c"]:
                elem = vitamins.find(v)
                record[f"vitamin_{v}"] = elem.text.strip() if elem is not None else ""

        minerals = food.find("minerals")
        if minerals is not None:
            for m in ["ca", "fe"]:
                elem = minerals.find(m)
                record[f"mineral_{m}"] = elem.text.strip() if elem is not None else ""

        foods.append(record)

    df = pd.DataFrame(foods)
    df.to_csv(f"{OUTPUT_DIR}/flattened_nutrition.csv", index=False)


with DAG(
    dag_id="flatten_json_xml_to_linear",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dz", "etl", "flatten"],
) as dag:

    t1 = PythonOperator(
        task_id="flatten_pets_json",
        python_callable=flatten_pets_json
    )

    t2 = PythonOperator(
        task_id="flatten_nutrition_xml",
        python_callable=flatten_nutrition_xml
    )

    [t1, t2]