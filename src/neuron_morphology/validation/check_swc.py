import os

SWC_EXPECTED_COLUMNS_READ = {'type', 'x', 'y', 'z', 'radius', 'parent'}
synonyms = {'r': 'radius'}
SWC_EXPECTED_COLUMNS_SAVE = {e if e not in synonyms else synonyms[e] for e in SWC_EXPECTED_COLUMNS_READ}


def parse_header_and_comments(file_path, max_=10, comment='#'):
    ignore = ['n', 'index']

    with open(file_path, 'r') as file:

        columns, comments = [], []
        for x in range(max_):
            line = file.readline().strip()

            line_parse = [i.lower() for i in line.split() if not i.startswith('#') and i not in ignore]
            line_parse = [e if e not in synonyms else synonyms[e] for e in line_parse]
            if SWC_EXPECTED_COLUMNS_SAVE.issubset(line_parse):
                columns = line_parse
            elif line.startswith(comment):
                comments.append(line)

        if not columns:
            raise ValueError(f'Could not parse columns in the first {max_} lines in {file}')

        return columns, comments


if __name__ == "__main__":

    init_path = "/Users/mouffok/work_dir/kg-inference-similarity/data_2"

    to_check = [os.path.join(init_path, i) for i in os.listdir(init_path) if i.startswith("morphologies_")]

    def per_path(path):

        paths_complete = dict(
            (p, [
                os.path.join(os.path.join(path, p), e)
                for e in os.listdir(os.path.join(path, p)) if "swc" in e
            ])
            for p in os.listdir(path)
        )

        for k, v in paths_complete.items():
            try:
                e = parse_header_and_comments(v[0])
            except ValueError as ex:
                print(f"failed {k}")

    for el in to_check:
        per_path(el)
