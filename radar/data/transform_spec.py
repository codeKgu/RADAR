"""Module for generating TableDeltaSpecs to transform a clean table to an unclean table."""

import difflib
import io
import warnings
from typing import List, Union

import pandas as pd

from radar.data import datamodel, perturb

# Suppress FutureWarnings
warnings.simplefilter(action="ignore", category=FutureWarning)


# def generate_transform_spec(
#     df_clean: Union[pd.DataFrame, List[pd.DataFrame]],
#     df_perturbed: pd.DataFrame,
#     convert_to_float: bool = True,
# ) -> perturb.TableDeltaSpec:
#     """Generates a TableDeltaSpec to transform a clean table to an unclean table."""
#     if isinstance(df_clean, list):
#         return generate_transform_spec_helper(
#             df_clean[0], df_perturbed, convert_to_float
#         )
#     else:
#         return generate_transform_spec_helper(df_clean, df_perturbed, convert_to_float)


# def generate_transform_spec_helper(
#     df_clean: pd.DataFrame,
#     df_perturbed: pd.DataFrame,
#     convert_to_float: bool = True,
# ) -> perturb.TableDeltaSpec:
#     """Generates a TableDeltaSpec to transform a clean table to an unclean table.

#     Args:
#       df_clean: The clean DataFrame.
#       df_perturbed: The perturbed DataFrame.
#       convert_to_float: Whether to convert the DataFrame to float.
#     Returns:
#       A TableDeltaSpec that describes the changes needed to transform the clean
#       table to the perturbed table.
#     """
#     if convert_to_float:
#         for col in df_clean.select_dtypes(include=["int", "float"]).columns:
#             df_clean.loc[:, col] = df_clean[col].astype(float)
#         for col in df_perturbed.select_dtypes(include=["int", "float"]).columns:
#             df_perturbed.loc[:, col] = df_perturbed[col].astype(float)
#     df_clean_str_lines = df_clean.to_csv(index=False).strip().splitlines()
#     df_perturbed_str_lines = df_perturbed.to_csv(index=False).strip().splitlines()

#     header = df_clean_str_lines[0]
#     columns = header.split(",")

#     sm = difflib.SequenceMatcher(
#         None, df_clean_str_lines[1:], df_perturbed_str_lines[1:]
#     )
#     insert_rows = []
#     overwrite_cells = []
#     for tag, i1, i2, j1, j2 in sm.get_opcodes():
#         if tag == "equal":
#             continue
#         elif tag == "insert":
#             for _, j in enumerate(range(j1, j2)):
#                 line = df_perturbed_str_lines[j + 1]
#                 values = pd.read_csv(io.StringIO(f"{header}\n{line}")).iloc[0].to_dict()
#                 values = {k: None if pd.isna(v) else v for k, v in values.items()}
#                 insert_rows.append(perturb.InsertRow(index=j, row=values))
#         elif tag == "replace":
#             for offset in range(min(i2 - i1, j2 - j1)):
#                 i = i1 + offset
#                 line1 = df_clean_str_lines[i + 1]
#                 line2 = df_perturbed_str_lines[j1 + offset + 1]
#                 row1 = pd.read_csv(io.StringIO(f"{header}\n{line1}"), dtype=str).iloc[0]
#                 row2 = pd.read_csv(io.StringIO(f"{header}\n{line2}"), dtype=str).iloc[0]
#                 for col in columns:
#                     val1 = row1[col]
#                     val2 = row2[col]
#                     if pd.isna(val1) and pd.isna(val2):
#                         continue
#                     if val1 != val2:
#                         overwrite_cells.append(
#                             perturb.OverwriteCell(
#                                 row=i,
#                                 col=col,
#                                 new_value=None if pd.isna(val2) else val2,
#                             )
#                         )
#             for j in range(j1 + (i2 - i1), j2):
#                 line = df_perturbed_str_lines[j + 1]
#                 values = pd.read_csv(io.StringIO(f"{header}\n{line}")).iloc[0].to_dict()
#                 values = {k: (None if pd.isna(v) else v) for k, v in values.items()}
#                 insert_rows.append(perturb.InsertRow(index=j, row=values))
#     return perturb.TableDeltaSpec(
#         insert_rows=insert_rows, overwrite_cells=overwrite_cells
#     )


def generate_transform_spec_delete_overwrite(
    df: pd.DataFrame,
    df_recovered: pd.DataFrame,
) -> perturb.TableDeltaSpec:
    """Generates a TableDeltaSpec to transform a clean table to an unclean table, focusing on deletions and overwrites.

    Args:
      df_clean: The clean DataFrame.
      df_perturbed: The perturbed DataFrame.
    Returns:
      A TableDeltaSpec that describes the changes needed to transform the clean
      table to the perturbed table, only including deletions and overwrites.
    """

    df_str_lines = df.to_csv(index=False).strip().splitlines()
    df_recovered_str_lines = df_recovered.to_csv(index=False).strip().splitlines()

    header = df_str_lines[0]
    columns = header.split(",")

    sm = difflib.SequenceMatcher(None, df_str_lines[1:], df_recovered_str_lines[1:])
    delete_rows = []
    overwrite_cells = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            continue
        elif tag == "delete":
            # Add all rows in the deleted range to delete_rows
            for i in range(i1, i2):
                delete_rows.append(i)
        elif tag == "replace":
            # Handle overwrites for matching rows
            for offset in range(min(i2 - i1, j2 - j1)):
                i = i1 + offset
                line1 = df_str_lines[i + 1]
                line2 = df_recovered_str_lines[j1 + offset + 1]
                row1 = pd.read_csv(io.StringIO(f"{header}\n{line1}"), dtype=str).iloc[0]
                row2 = pd.read_csv(io.StringIO(f"{header}\n{line2}"), dtype=str).iloc[0]
                for col in columns:
                    val1 = row1[col]
                    val2 = row2[col]
                    if pd.isna(val1) and pd.isna(val2):
                        continue
                    if val1 != val2:
                        overwrite_cells.append(
                            perturb.OverwriteCell(
                                row=i,
                                col=col,
                                new_value=None if pd.isna(val2) else val2,
                            )
                        )
            # If there are more rows in clean than perturbed, delete the extra rows
            if i2 - i1 > j2 - j1:
                for i in range(i1 + (j2 - j1), i2):
                    delete_rows.append(i)

    return perturb.TableDeltaSpec(
        delete_rows=delete_rows, overwrite_cells=overwrite_cells
    )


# if __name__ == "__main__":
# # Example 1: Basic test with deletions and overwrites
# df_clean = pd.DataFrame(
#     {
#         "id": [1, 2, 3, 4, 5],
#         "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
#         "age": [25, 30, 35, 40, 45],
#         "score": [85.5, 92.0, 78.5, 88.0, 95.5],
#     }
# )

# # Create a perturbed version with:
# # - Deleted row (id=3)
# # - Modified values (Bob's age and score)
# # - Modified value (Eve's name)
# df_perturbed = pd.DataFrame(
#     {
#         "id": [1, 2, 4, 5],
#         "name": ["Alice", "Bob", "David", "Eva"],
#         "age": [25, 32, 40, 45],
#         "score": [85.5, 94.0, 88.0, 95.5],
#     }
# )

# # Test the original function
# spec_original = generate_transform_spec(df_clean, df_perturbed)
# print("\nOriginal Transform Spec:")
# print(f"Insert Rows: {spec_original.insert_rows}")
# print(f"Overwrite Cells: {spec_original.overwrite_cells}")

# # Test the delete/overwrite function
# spec_delete_overwrite = generate_transform_spec_delete_overwrite(
#     df_clean, df_perturbed
# )
# print("\nDelete/Overwrite Transform Spec:")
# print(f"Delete Rows: {spec_delete_overwrite.delete_rows}")
# print(f"Overwrite Cells: {spec_delete_overwrite.overwrite_cells}")

# # Test recovering the original DataFrame
# df_recovered = apply_delete_spec(df_perturbed, spec_delete_overwrite)
# print("\nRecovered DataFrame:")
# print(df_recovered)
# print("\nOriginal DataFrame:")
# print(df_clean)
# print("\nAre they equal?", df_recovered.equals(df_clean))

# # Example 2: Test with more complex changes
# df_clean2 = pd.DataFrame(
#     {
#         "id": [1, 2, 3, 4, 5, 6],
#         "name": ["John", "Mary", "Peter", "Sarah", "Tom", "Lisa"],
#         "age": [20, 25, 30, 35, 40, 45],
#         "score": [90.0, 85.5, 92.0, 88.5, 95.0, 87.0],
#     }
# )

# # Create a perturbed version with:
# # - Multiple deleted rows (id=2, 4)
# # - Multiple modified values
# df_perturbed2 = pd.DataFrame(
#     {
#         "id": [1, 3, 5, 6],
#         "name": ["John", "Petra", "Tom", "Lisa"],
#         "age": [20, 31, 41, 45],
#         "score": [91.0, 92.0, 94.0, 87.0],
#     }
# )

# print("\n=== Example 2 ===")
# spec2 = generate_transform_spec_delete_overwrite(df_clean2, df_perturbed2)
# print("\nDelete/Overwrite Transform Spec (Example 2):")
# print(f"Delete Rows: {spec2.delete_rows}")
# print(f"Overwrite Cells: {spec2.overwrite_cells}")

# # Test recovering the original DataFrame for example 2
# df_recovered2 = apply_delete_spec(df_perturbed2, spec2)
# print("\nRecovered DataFrame (Example 2):")
# print(df_recovered2)
# print("\nOriginal DataFrame (Example 2):")
# print(df_clean2)
# print("\nAre they equal?", df_recovered2.equals(df_clean2))
