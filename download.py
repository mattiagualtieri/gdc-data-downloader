import yaml
import csv
import json
import os
import subprocess
from case import Case


def get_manifest_filenames(manifest):
    filenames = []
    with open(manifest) as manifest_file:
        csv_reader = csv.reader(manifest_file, delimiter='\t')
        for row in csv_reader:
            filenames.append(row[1])
    return filenames


def write_tmp_manifest(config, cases, manifest, data_type):
    case_list = list(cases.items())[:config['max_downloads']]
    tmp_manifest = os.path.join(config['manifest_dir'], f'manifest-{data_type}-tmp.txt')
    with open(tmp_manifest, 'w') as file:
        file.write('id\tfilename\tmd5\tsize\tstate\n')
        for case in case_list:
            filename = case[1].get_filename(data_type)
            with open(manifest) as manifest_file:
                for line in manifest_file:
                    if filename in line:
                        file.write(line)
    return tmp_manifest


def download_data(config, data_type, manifest):
    output_dir = config[data_type]['output_dir']
    if os.listdir(output_dir):
        print(f'Directory {output_dir} is not empty, skipping download')
        os.remove(manifest)
        return
    print(f'Downloading {data_type} data from GDC...')
    subprocess.run(["gdc-client", "download", "-m", manifest, "-d", output_dir])
    os.remove(manifest)
    print(f'Successfully downloaded {data_type} data')


def search_file(directory, to_search):
    for subdir in os.listdir(directory):
        subdir_path = os.path.join(directory, subdir)
        if os.path.isdir(subdir_path):
            for filename in os.listdir(subdir_path):
                file_path = os.path.join(subdir_path, filename)
                if os.path.isfile(file_path) and filename == to_search:
                    return file_path


def parse_meth(file, header=False):
    result = []
    with open(file) as meth_file:
        csv_reader = csv.reader(meth_file, delimiter='\t')
        for row in csv_reader:
            if header:
                result.append(row[0])
                result.append(';')
            else:
                result.append(row[1])
                result.append(';')
    return ''.join(result)


def parse_genexpr(file, header=False):
    result = []
    line_count = 0
    with open(file) as genexpr_file:
        csv_reader = csv.reader(genexpr_file, delimiter='\t')
        for row in csv_reader:
            line_count += 1
            if line_count >= 7:
                if header:
                    result.append(row[1])
                    result.append(';')
                else:
                    result.append(row[8])
                    result.append(';')
    return ''.join(result)


def main():
    with open('config/download.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    print('Successfully loaded configuration')

    starting_from = config['starting_from']
    if starting_from not in ['genexpr', 'meth']:
        raise RuntimeError('Not supported')
    print('Downloading data starting from: %s data' % starting_from)

    master = starting_from
    slave = 'meth' if starting_from == 'genexpr' else 'genexpr'
    master_manifest = config[master]['manifest']
    slave_manifest = config[slave]['manifest']
    master_mapping = config[master]['mapping']
    slave_mapping = config[slave]['mapping']

    filenames = get_manifest_filenames(master_manifest)

    cases = {}
    with open(master_mapping, 'r') as json_file:
        mapping = json.load(json_file)
        for case in mapping:
            filename = case["file_name"]
            if filename in filenames:
                case_id = case["cases"][0]["case_id"]
                if master == 'genexpr':
                    cases[case_id] = Case(case_id, genexpr_filename=filename)
                else:
                    cases[case_id] = Case(case_id, meth_filename=filename)

    print(f'Found a total of {len(cases)} cases')

    print(f'Searching {len(cases)} cases inside {slave_mapping}')
    with open(slave_mapping, 'r') as json_file:
        mapping = json.load(json_file)
        for case in mapping:
            filename = case["file_name"]
            case_id = case["cases"][0]["case_id"]
            if case_id in cases:
                cases[case_id].set_filename(filename, slave)

    complete_cases = {k: v for k, v in cases.items() if v.is_complete()}
    print(f'Keeping only complete cases: {len(complete_cases)}')

    max_downloads = config['max_downloads']
    print(f'Maximum downloads: {max_downloads}')

    # Master Manifest (TMP)
    master_tmp_manifest = write_tmp_manifest(config, complete_cases, master_manifest, master)

    # Slave Manifest (TMP)
    slave_tmp_manifest = write_tmp_manifest(config, complete_cases, slave_manifest, slave)

    # Download Master data
    download_data(config, master, master_tmp_manifest)

    # Download Slave data
    download_data(config, slave, slave_tmp_manifest)

    master_dir = config[master]['output_dir']
    slave_dir = config[slave]['output_dir']

    final_csv = os.path.join(config['processed_dir'], f'input.csv')
    header = True
    with open(final_csv, 'w') as final_csv_file:
        for subdir in os.listdir(master_dir):
            subdir_path = os.path.join(master_dir, subdir)
            if os.path.isdir(subdir_path):
                for filename in os.listdir(subdir_path):
                    file_path = os.path.join(subdir_path, filename)
                    if os.path.isfile(file_path) and filename.endswith(config[master]['filename_ending']):
                        case_id = next((key for key, case in cases.items() if case.get_filename(master) == filename), None)
                        slave_filename = cases[case_id].get_filename(slave)
                        slave_file = search_file(slave_dir, slave_filename)
                        if header:
                            final_csv_file.write('case_id')
                            final_csv_file.write(';')
                            final_csv_file.write(parse_genexpr(slave_file, header=True))
                            final_csv_file.write(';')
                            final_csv_file.write(parse_meth(file_path, header=True))
                            final_csv_file.write('\n')
                            header = False
                        final_csv_file.write(case_id)
                        final_csv_file.write(';')
                        final_csv_file.write(parse_genexpr(slave_file))
                        final_csv_file.write(';')
                        final_csv_file.write(parse_meth(file_path))
                        final_csv_file.write('\n')

    print('Execution terminated successfully')


if __name__ == '__main__':
    main()
