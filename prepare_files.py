#!/usr/bin/env python3

from typing import Dict, List, IO, Tuple
import argparse
import pathlib
import re
import shutil
import sys

from fuzzywuzzy import fuzz


def parse_schema(sql_file: pathlib.Path) -> Dict[str, List[str]]:
    """
    Parses the SQL schema creation script (schemas.sql)
    """
    in_comment = False
    in_table_schema = False
    schema = {}
    table_schema: List[str] = []
    with open(sql_file, "r", encoding="utf-8") as in_fd:
        for line in in_fd:
            line = line.strip().lower()
            if "/*" in line:
                # Start of comment
                in_comment = True
                continue
            elif "*/" in line:
                # End of comment
                in_comment = False
                continue
            elif in_comment:
                # Ignore comments
                continue
            elif line == ");":
                # End of schema
                in_table_schema = False
            elif line.startswith("create table "):
                # Starting a table
                match = re.search(r"create table (\w+) \(", line)
                if match is not None:
                    in_table_schema = True
                    table = match.group(1)
                    table_schema = []
                    schema[table] = table_schema
            elif in_table_schema:
                # Line in the current schema
                col_name = line.split()[0]
                table_schema.append(col_name)

    return schema


def table_files(sql_file: pathlib.Path) -> Dict[str, str]:
    """
    Associates the name of an output file to its SQL table, by reading the
    snb-load.sql script
    """
    results = {}
    with open(sql_file, "r", encoding="utf-8") as in_fd:
        for line in in_fd:
            line = line.strip()
            if not line.startswith("COPY"):
                # Ignore lines we don't care about
                continue

            # Find important indices
            table_start_idx = len("COPY ")
            table_end_idx = line.index(" FROM ", table_start_idx)
            csv_start_idx = line.index("'", table_end_idx + len("FROM"))
            csv_end_idx = line.index("'", csv_start_idx + 1)

            # Extract the table name
            table_name = line[table_start_idx:table_end_idx].strip()
            if "(" in table_name:
                # Got a set of columns with the table name
                table_name = table_name[: table_name.index("(")].strip()

            # Extract the name of the CSV file (full Docker path)
            csv_path = line[csv_start_idx + 1 : csv_end_idx]
            csv_name = csv_path.rsplit("/", 1)[-1]

            # Store it
            results[csv_name] = table_name

    return results


def find_root(folder: pathlib.Path) -> pathlib.Path:
    """
    Finds the root of the dataset files

    :param folder: Folder to check
    :return: The folder parent of the "static" and "dynamic" folders
    :raise IOError: Path not found
    """
    if not folder.is_dir():
        raise IOError(f"{folder} is not a directory")

    if (folder / "static").exists() and (folder / "dynamic").exists():
        # Found it
        return folder

    for child in folder.iterdir():
        if folder.is_dir():
            try:
                return find_root(child)
            except IOError:
                continue
    else:
        raise IOError(f"Root not found under {folder}")


def read_line(in_fd: IO[bytes]) -> bytes:
    """
    Reads a single line from a file in bytes mode

    :param in_fd: Input file descriptor
    :return: The line (from in_fd current position), without EOL
    """
    result = []
    while True:
        # Ignore the first line
        read_char = in_fd.read(1)
        if not read_char or read_char == b"\n":
            # Don't include EOL or EOF
            break

        result.append(read_char)

    return b"".join(result)


def update_header(
    table: str, schema: Dict[str, List[str]], header: str
) -> str:
    """
    Returns the fixed header for the given file
    """
    # Table name
    table_name = table.lower()
    try:
        table_schema = schema[table_name][:]
    except KeyError:
        # Unknown schema
        print("Skipping unknown schema:", table_name)
        return header

    # Compute the prefix of each column
    prefix = "".join(t[0].lower() for t in table.split("_"))

    # Work on each column title
    raw_titles = header.split("|")
    initial_titles = raw_titles[:]
    for idx in range(len(raw_titles)):
        # Special case: we now the ID column must be prefixed
        raw_title = raw_titles[idx]
        if raw_title == "id":
            # Special case
            raw_title = f"{table_name}id"

        # Add the prefix
        raw_titles[idx] = f"{prefix}_{raw_title}"

    if len(raw_titles) > len(table_schema):
        print(
            "Got too many columns in",
            table,
            ": got",
            len(raw_titles),
            "columns, expected",
            len(table_schema),
            file=sys.stderr,
        )

    # Find the best match for each column
    best_matches: Dict[str, List[Tuple[int, str]]] = {}
    for raw_title in raw_titles:
        low_title = raw_title.lower()
        title_matches: List[Tuple[int, str]] = []
        # Give some hints for string comparison
        for title in {
            raw_title,
            low_title.replace("post", "message"),
            low_title.replace("comment", "message"),
            low_title.replace("university", "organisation"),
            low_title.replace("locationplace", "place"),
            low_title.replace("containerforum", "forum"),
        }:
            for col_name in table_schema:
                ratio = fuzz.ratio(title, col_name)
                if ratio > 70 or any(
                    low_title in col_name.lower()
                    or col_name.lower() in low_title
                    for col_name in table_schema
                ):
                    # Empirical: got invalid matches at 62, valid at 64
                    title_matches.append((ratio, col_name))

        if title_matches:
            best_matches[raw_title] = sorted(title_matches, reverse=True)

    # Get only the best matches
    best_guesses = sorted(
        best_matches.items(), key=lambda x: x[1][0], reverse=True
    )[: len(table_schema)]

    # Back to a dictionary: current title -> schema
    matches: Dict[str, str] = {t[0]: t[1][0][1] for t in best_guesses}

    new_titles = [
        matches.get(raw_title, raw_title) for raw_title in raw_titles
    ]

    not_found = sorted(
        initial_titles[idx]
        for idx, raw_title in enumerate(raw_titles)
        if raw_title not in matches
    )
    if not_found:
        print(
            "Missed convertion of", len(not_found), "columns for",
            table_name,
            ":",
            ", ".join(not_found),
            file=sys.stderr,
        )

    return "|".join(new_titles)


def find_closest(value: str, possible_values: List[str]) -> str:
    """
    Finds the closest match for the given value

    :param value: Known value (dirty)
    :param possible_values: Possible clean values
    :return: The closest match
    """
    return max(
        (fuzz.ratio(value, possible_value), possible_value)
        for possible_value in possible_values
    )[1]


def merge_files(
    folder: pathlib.Path,
    output_file: pathlib.Path,
    schema: Dict[str, List[str]],
    csv_table: Dict[str, str],
):
    """
    Merges the CSV files from the given folder to a single output file

    :param folder: Folder where to find CSV files
    :param output_file: Path to the merged CSV file
    :param schema: DB schema
    :param csv_table: A dictionary: CSV file -> Table name
    """
    # Compute the table name
    for table_name in (
        folder.stem,
        folder.stem.lower(),
        csv_table[find_closest(output_file.name, list(csv_table))],
    ):
        if table_name in schema:
            print(
                "Found",
                folder.name,
                "in schema:",
                table_name,
                "-",
                ", ".join(schema[table_name]),
            )
            break
    else:
        print("No schema found for folder", folder.stem, file=sys.stderr)
        table_name = folder.stem

    with open(output_file, "wb") as out_fd:
        for file_idx, file_part in enumerate(sorted(folder.glob("*.csv"))):
            with open(file_part, "rb") as in_fd:
                if file_idx == 0:
                    # Work the header
                    raw_header = read_line(in_fd).decode("utf8")
                    new_header = update_header(table_name, schema, raw_header)
                    out_fd.write(new_header.encode("utf8"))
                    out_fd.write(b"\n")
                else:
                    # Ignore header of other files
                    read_line(in_fd)

                shutil.copyfileobj(in_fd, out_fd)


def run(folder: pathlib.Path, ddl_folder: pathlib.Path) -> int:
    """
    Does the job

    :param folder: Folder where to look into
    :param ddl_folder: Path to the folder containing the SQL scripts
    :return: An error code
    """
    if not folder.exists():
        print("Folder not found:", folder, file=sys.stderr)
        return 1

    # Parse the loading script
    csv_table = table_files(ddl_folder / "snb-load.sql")

    # Parse the schema
    schema = parse_schema(ddl_folder / "schema.sql")

    # Ensure we are at the right place
    root = find_root(folder)

    nb_files = 0
    for part_name in ("static", "dynamic"):
        print("Working on", part_name, "part...")
        part = root / part_name
        for child in part.iterdir():
            if child.is_dir():
                print("... merging", child.stem, "...")
                merge_files(
                    child,
                    part / f"{child.stem.lower()}_0_0.csv",
                    schema,
                    csv_table,
                )
                nb_files += 1

    print("Worked on", nb_files, "files.")
    return 0


def main(argv=None):
    """
    Entry point

    :param argv: Arguments lists (or None)
    :return: An error code
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ddl",
        required=True,
        type=pathlib.Path,
        help="Path to the folder containing the SQL scripts (ddl)",
    )
    parser.add_argument(
        "folder", type=pathlib.Path, help="Folder where to find the CSV files"
    )
    options = parser.parse_args(argv)

    return run(options.folder, options.ddl)


if __name__ == "__main__":
    sys.exit(main() or 0)
