import os
import shutil
import subprocess
import yaml
from utils.manifest import Manifest
from utils.mapping import MappingList
from utils.data_merger import DataMerger


def load_manifests(config):
    manifests = []
    for download in config['downloads']:
        manifest_path = download['manifest']
        with open(manifest_path) as file:
            manifest = Manifest(name=download['data'], file=file)
            print(f'Loaded: {manifest}')
            manifests.append(manifest)
    return manifests


def load_mappings(config):
    mappings = []
    for download in config['downloads']:
        mapping_path = download['mapping']
        with open(mapping_path) as file:
            mapping = MappingList(name=download['data'], file=file)
            print(f'Loaded: {mapping}')
            mappings.append(mapping)
    return mappings


def generate_temp_manifest(config, mapping_list, manifest):
    temp_manifest = os.path.join(config['manifest_dir'], f'temp_manifest_{manifest.name}.txt')
    with open(temp_manifest, 'w') as file:
        file.write('id\tfilename\tmd5\tsize\tstate\n')
        for case_id in mapping_list.get_cases():
            mapping = mapping_list.mappings[case_id][manifest.name]
            filename = mapping.filename
            with open(manifest.file_path) as manifest_file:
                for line in manifest_file:
                    if filename in line:
                        file.write(line)
    return manifest.name, temp_manifest


def download_data(config, data_type, manifest):
    output_dir = os.path.join(config['raw_data_dir'], data_type)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if os.listdir(output_dir):
        print(f'Directory {output_dir} is not empty, skipping download')
        os.remove(manifest)
        return
    print(f'Downloading {data_type} data from GDC...')
    subprocess.run(["gdc-client", "download", "-m", manifest, "-d", output_dir, "-n", "2"])
    os.remove(manifest)
    print(f'Successfully downloaded {data_type} data')


def move_all_wsi_to_dir(input_dir, output_dir):
    for subdir in os.listdir(input_dir):
        new_name = ''
        svs_file = ''
        subdir_path = os.path.join(input_dir, subdir)
        if os.path.isdir(subdir_path):
            for file in os.listdir(subdir_path):
                if file.endswith('.txt'):
                    txt_file = os.path.join(subdir_path, file)
                    with open(txt_file, 'r') as f:
                        lines = f.readlines()
                        if len(lines) > 1:
                            # Assuming the second line of the TSV file contains the CASE value
                            case_id = lines[1].split('\t')[3].strip()  # Adjust index if necessary
                            new_name = os.path.join(output_dir, case_id + '.svs')
                elif file.endswith('.svs'):
                    svs_file = os.path.join(subdir_path, file)
            shutil.move(svs_file, new_name)


def main():
    with open('config/download.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    print('Successfully loaded configuration')

    manifests = load_manifests(config)
    mapping_lists = load_mappings(config)

    merged_mapping_list = MappingList(name='merged')
    for mapping_list in mapping_lists:
        for case_id in mapping_list.mappings:
            mapping = mapping_list.mappings[case_id][mapping_list.name]
            merged_mapping_list.add_mapping(case_id, mapping)

    merged_mapping_list.remove_incomplete(len(config['downloads']))
    print(f'Loaded: {merged_mapping_list}')

    max_downloads = config['max_downloads']
    print(f'Maximum downloads: {max_downloads}')

    merged_mapping_list.cut(max_downloads)

    temp_manifests = []
    for manifest in manifests:
        temp_manifest = generate_temp_manifest(config, merged_mapping_list, manifest)
        temp_manifests.append(temp_manifest)

    for data_type, temp_manifest in temp_manifests:
        download_data(config, data_type, temp_manifest)

    merger = DataMerger(config['raw_data_dir'], merged_mapping_list, config['output'])
    merger.merge()

    input_dir = os.path.join(config['raw_data_dir'], 'wsi')
    output_dir = config['wsi_data_dir']
    print(f'Moving WSIs from {input_dir} to {output_dir}')
    move_all_wsi_to_dir(input_dir, output_dir)

    print('Execution terminated successfully')


if __name__ == '__main__':
    main()
