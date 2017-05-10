from typing import List

CHUNK_SIZE = 100


def read_chunk_lines(input_lines: str, chunk_size: int) -> List[str]:
    first_char = 0
    last_char = 0
    current_count = 0

    for char in input_lines:
        last_char += 1
        if char == '\n':
            current_count += 1
            if current_count == chunk_size:
                yield input_lines[first_char:last_char]
                first_char = last_char
                current_count = 0

    last_line = input_lines[first_char:last_char]
    if last_line != '':
        yield last_line
