import glob
import os
from typing import List, Literal, Tuple

import dotenv
import pandas as pd
from datasets import load_dataset
from tqdm import tqdm

from radar import utils
from radar.data import datamodel

dotenv.load_dotenv()


def load_task_instances_hf(
    split: Literal["full", "tasks", "sizes"],
) -> List[datamodel.TaskInstance]:
    if split == "full":
        suffix = ""
    else:
        suffix = f"-{split}"
    ds = load_dataset("kenqgu/radar", data_dir=f"radar{suffix}")
    df_summary = ds["test"].to_pandas()
    return [datamodel.TaskInstance(**d) for d in ds["test"]], df_summary


def load_task_instances_local(
    task_dir: str,
) -> List[datamodel.TaskInstance]:
    task_instances = []
    df_list = []
    for file in glob.glob(os.path.join(task_dir, "*.json")):
        task_instances.append(datamodel.TaskInstance(**utils.read_json(file)))
    return task_instances


if __name__ == "__main__":
    task_instances = load_task_instances_local(
        "/projects/bdata/kenqgu/Research/Year4/radar_release/RADAR/task_example/influenza-like-illness/tasks"
    )
    print(len(task_instances))
