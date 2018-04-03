import os
import re
import shutil
import logging
import tarfile
from random import random
from urllib.request import urlretrieve
from progressbar import ProgressBar, Percentage, Bar


class FileSystemManager:

    def __init__(self, source_dir=None):
        self.source_dir = source_dir
        self.archive_dir = None

    def clean_run(self):

        for directory in self.source_dir:
            if directory:
                if os.path.exists(directory):
                    try:
                        logging.info("Removing resource: Directory [%s].", os.path.abspath(directory))
                        shutil.rmtree(directory)
                    except OSError:
                        logging.error(
                            "Could not remove resource: Directory [%s].", os.path.abspath(directory))

    def extract_archive(self, archive):
        

        self.archive_dir = archive.split('.')[0]

        if not os.path.exists(self.archive_dir):
            logging.info("Extracting archive %s to %s", archive, os.path.abspath(self.archive_dir))

            if archive.lower().endswith('.tar.gz'):
                tar = tarfile.open(archive, "r:gz")
            else:
                logging.error("File extension not currently supported.")
                return

            tar.extractall()
            tar.close()

        return self.archive_dir

    def remove_files_except(self, extension):
        

        for root, dirs, files in os.walk(self.archive_dir):
            for current_file in files:

                if not current_file.lower().endswith(extension):

                    try:
                        logging.debug("Removing resource: File [%s]", os.path.join(root, current_file))
                        os.remove(os.path.join(root, current_file))
                    except OSError:
                        logging.error("Could not remove resource: File [%s]", os.path.join(root, current_file))

    def data_science_fs(self, category0, category1):
        

        for new_dir in ['train', 'predict']:
            for new_category in [category0, category1]:

                abspath_dir = os.path.abspath(os.path.join(self.source_dir, new_dir, new_category))

                logging.info(
                    "Creating resource: Directory [%s]", abspath_dir)
                os.makedirs(abspath_dir)

    def organise_files(self, directory, category_rules):
        

        predict_ratio = 0.1

        for root, dirs, files in os.walk(directory):
            for file in files:

                if re.compile(list(category_rules.values())[0]).match(file):

                    if random() < predict_ratio:
                        train_test_dir = 'predict/'

                    else:
                        train_test_dir = 'train/'

                    try:
                        logging.debug(
                            "Moving %s from %s to %s", file, root,
                            os.path.join(self.source_dir, train_test_dir, list(category_rules.keys())[0]))

                        os.rename(
                            os.path.join(root, file),
                            os.path.join(self.source_dir, train_test_dir, list(category_rules.keys())[0], file))

                    except OSError:
                        logging.error("Could not move %s ", os.path.join(root, file))

                elif re.compile(list(category_rules.values())[1]).match(file):

                    if random() < predict_ratio:
                        train_test_dir = 'predict/'

                    else:
                        train_test_dir = 'train/'

                    try:
                        logging.debug("Moving %s from %s to %s", file, root,
                                      os.path.join(self.source_dir, train_test_dir, list(category_rules.keys())[1]))

                        os.rename(
                            os.path.join(root, file),
                            os.path.join(self.source_dir, train_test_dir, list(category_rules.keys())[1], file))

                    except OSError:
                        logging.error("Could not move %s ", os.path.join(root, file))

                else:
                    logging.error("No files matching category regex")

        try:
            logging.info("Removing resource: Directory [%s].", os.path.abspath(self.archive_dir))
            shutil.rmtree(self.archive_dir)
        except OSError:
            logging.error("Could not remove resource: Directory [%s].", os.path.abspath(self.archive_dir))


class DownloadManager:

    def __init__(self, download_url):
        self.download_url = download_url
        self.source_data = download_url.split('/')[-1]
        self.source_data = os.path.abspath(self.source_data)

    def download(self):
        

        def progress(count, blockSize, totalSize):
            pbar.update(int(count * blockSize * 100 / totalSize))

        if not os.path.exists(self.source_data):
            logging.info(
                "%s not found on local filesystem. File will be downloaded from %s.",
                self.source_data, self.download_url)

            pbar = ProgressBar(widgets=[Percentage(), Bar()])
            urlretrieve(self.download_url, self.source_data, reporthook=progress)



def main():

    logging.basicConfig(level=logging.INFO)

    source_data = 'http://www.inf.ufpr.br/vri/databases/BreaKHis_v1.tar.gz'
    source_archive = source_data.split('/')[-1]
    image_directory = 'images'


    file_manager = FileSystemManager(image_directory)
    file_manager.clean_run()

    download_manager = DownloadManager(source_data)
    download_manager.download()

    extract_dir = file_manager.extract_archive(source_archive)
    file_manager.remove_files_except('.png')
    file_manager.data_science_fs(category0='benign', category1='malignant')
    file_manager.organise_files(extract_dir, category_rules={'benign': 'SOB_B_.*.png', 'malignant': 'SOB_M_.*.png'})


if __name__ == '__main__':
    main()
