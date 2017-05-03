from typing import List

CHUNK_SIZE = 50


def read_chunk_lines(input_lines: str, chunk_size: int) -> List[str]:
    lines = input_lines.splitlines()  # split on newlines

    """Yield successive chunk_size-sized chunks from lines."""
    for i in range(0, len(lines), chunk_size):
        yield lines[i:i + chunk_size]
