import argparse

from enre import settings
from enre.gdrive import GDrive


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--processes', default=10, type=int)

    args = parser.parse_args()
    gdrive = GDrive()
    print(f'Downloading into {settings.DATA_PATH}')
    gdrive.download_all_files_parallel_2(settings.DATA_PATH, skip_existing=True, processes=args.processes)


if __name__ == '__main__':
    main()
