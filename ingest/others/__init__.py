CHUNK_SIZE = 50


def read_chunk_lines(input_lines, n):
    lines = input_lines.splitlines()

    """Yield successive n-sized chunks from l."""
    for i in range(0, len(lines), n):
        yield lines[i:i + n]
