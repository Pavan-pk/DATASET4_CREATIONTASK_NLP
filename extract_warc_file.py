import collections
import glob
import gzip
import json
import os
import re
import uuid
import wget
import csv
import shutil
import random

from bs4 import BeautifulSoup

URL_BLACKLIST = set()
HEADER = ["uuid", "image_url", "local_path", "alt_text", "context", "url", "segment", "warc_file"]
PATHS_FILE = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-31/warc.paths.gz"
DOWNLOAD_PREFIX = "https://commoncrawl.s3.amazonaws.com/"

BLACKLIST_FOLDER = "blacklist_domains"
#  Fill back_list urls
BLACKLIST_FILES = os.listdir(BLACKLIST_FOLDER)
for domains_list in BLACKLIST_FILES:
    with open(os.path.join(BLACKLIST_FOLDER, domains_list), 'r') as fd:
        lines = fd.readlines()
        for line in lines:
            URL_BLACKLIST.add(line.rstrip())


def process_html(url, content, directory_prefix=''):
    """Processes single html webpage and extracts instructions as tasks."""

    domain = url.split('://')[1].split('/')[0]
    soup = BeautifulSoup(content, 'html.parser')
    returnme = list()
    # Remove unnecessary tags which could exist in <ol>
    for s in soup.select('script'):
        s.extract()
    for s in soup.select('noscript'):
        s.extract()
    for s in soup.select('table'):
        s.extract()
    for s in soup.select('figure'):
        s.extract()

    if domain == 'www.lifewire.com':
        for s in soup.find_all('div', {'class': 'theme-experttiptip'}):
            s.extract()
        for s in soup.find_all('div', {'class': 'theme-experttipimportant'}):
            s.extract()

    # For specific websites, need fine tune the parser to remove (.extract()) some
    # unnecessary tags to clean up the result got from ol.get_text()
    # if domain == 'www.wikihow.com':
    #     for s in soup.select('span'):
    #         s.extract()

    ols = soup.find_all('ol')

    for _, ol in enumerate(ols):
        if ol.find_all('img'):
            spans_in_image = _replace_unicode_with_space(ol.get_text(' ', strip=True)).split("\n")
            # Only take in context texts which are more than 10 words (change later to higher number if required).
            spans_in_image = [i for i in spans_in_image if len(i) > 10]
            # no context available
            if not spans_in_image:
                continue
            segment, warc_file_name = "", ""
            if directory_prefix:
                segment = directory_prefix.split("_")[0]
                warc_file_name = directory_prefix.split("_")[1]
            for image_tag in ol.find_all('img'):
                if image_tag['alt'] != "":
                    try:
                        image_url = image_tag["src"]
                        # check if the image is hosted on web.
                        if not image_url.startswith("http"):
                            image_url = image_tag.get("longdesc")
                            if not image_url.startswith("http"):
                                continue
                        image_filename = wget.download(image_url, out=directory_prefix)
                        # Add context filtering here.
                        # Check the context of the image and filter spans_in_image if the filter causes spans_in_image
                        # to be empty then continue to next image.
                        my_uuid = str(uuid.uuid4())
                        local_path = directory_prefix + image_filename if directory_prefix else image_filename
                        alt_text = image_tag['alt']
                        context = spans_in_image[0] + ','.join(spans_in_image[1:])
                        web_url = url
                        warc_segment = segment
                        warc_fn = warc_file_name
                        write_list = [my_uuid, image_url, local_path, alt_text, context, web_url, warc_segment, warc_fn]
                        returnme.append(write_list)
                    except Exception as e:
                        os.remove(image_filename)
                        continue

    return returnme


def _replace_unicode_with_space(text):
    """Replaces all unwanted unicode chars with single space."""
    returnme = ''.join([i if ord(i) < 128 else ' ' for i in text])
    returnme = ' '.join(returnme.split())  # Change all space/newline to one space
    return returnme


def _is_valid(url, inst):
    url_words = re.compile(r'\w+').findall(url.lower())
    instruction_words = re.compile(r'\w+').findall(inst.lower())

    phone_set = {'android', 'phone', 'iphone'}
    click_set = {'tap', 'click'}

    return (set(url_words + instruction_words).intersection(phone_set) and
            set(instruction_words).intersection(click_set))


# DomainStatsIdx
COUNT_IN_WARC = 0
COUNT_IS_RESPONSE = 1
COUNT_HTML = 2
COUNT_HTML_HAS_INST = 3
COUNT_INST = 4


def _parse_one_page(lines, stats, domain_stats, download_dir=None):
    """Parses one page in warc file.
  Args:
    lines: the lines of WARC content to parse, which should contain single web
      interaction info, such as a request or a response
    stats: dict of {string, int}, for reason of failure and count
    domain_stats: dict of {domain: [a, b, c, d, e]} which are the counts of
      different DomainStatsIdx items for each domain
  Returns:
    list of triple (url, instruction, html_content) for each instruction found.
  """
    if not lines:
        return []
    if lines[0].strip() != 'WARC/1.0':
        stats['Error_no_WARC/1.0_in_head'] += 1
        return []

    url = None
    warc_type = None
    section = 1
    html_lines = []
    for _, line in enumerate(lines):
        line = line.strip()
        if section < 3:
            if not line:
                section += 1
        if section == 1:
            if line.startswith('WARC-Type: '):
                warc_type = line[len('WARC-Type: '):].strip()
            if line.startswith('WARC-Target-URI: '):
                url = line[len('WARC-Target-URI: '):].strip()
                # Extract support.google.com from
                # https://support.google.com/news/publisher-center/answer/9603942
                domain = url.split('://')[1].split('/')[0]
                if domain in URL_BLACKLIST:
                    return []
                # skip blacklisted urls (NSFW)
                # if FLAGS.filter_domain:
                #     if domain not in URL_WHITE_LIST:
                #         stats['NotFound_Domain_mismatch'] += 1
                #         return []
                domain_stats['DOMAIN_' + domain][COUNT_IN_WARC] += 1
                if warc_type == 'response':
                    domain_stats['DOMAIN_' + domain][COUNT_IS_RESPONSE] += 1

        if section == 3 and line:  # section 3 is html:
            html_lines.append(line)
    if not url or not html_lines:
        stats['No_HTML'] += 1
        return []

    domain_stats['DOMAIN_' + domain][COUNT_HTML] += 1

    try:
        html_content = '\n'.join(html_lines)
        instructions = process_html(url, html_content, download_dir)
    except Exception:  # pylint: disable=broad-except
        stats['Error_parse_html'] += 1
        return []

    if not instructions:
        stats['No_instruction'] += 1
        return []

    stats['Got'] += 1
    domain_stats['DOMAIN_' + domain][COUNT_HTML_HAS_INST] += 1
    domain_stats['DOMAIN_' + domain][COUNT_INST] += len(
        instructions)
    return instructions


def extract_instructions_from_warc_file(warc_file_path, file_handler, download_dir=""):
    """Reads instruction from WARC file.
  Args:
    warc_file_path: warc file path.
    file_handler: file handler of the warc file.
  Yields:
    triple(url, index, instruction)
  """
    lines_of_one_page = []
    stats = collections.defaultdict(int)
    domain_stats = collections.defaultdict(lambda: [0, 0, 0, 0, 0])

    for line in file_handler:
        if line.strip().startswith('WARC/1.0'):
            stats['Total'] += 1
            urls_and_instructions = _parse_one_page(lines_of_one_page,
                                                    stats, domain_stats, download_dir=download_dir)
            for csv_row in urls_and_instructions:
                yield csv_row
            lines_of_one_page = [line]
        else:
            lines_of_one_page.append(line)

    urls_and_instructions = _parse_one_page(lines_of_one_page,
                                            stats, domain_stats, download_dir=download_dir)
    stats['file_name'] = warc_file_path

    # if FLAGS.filter_domain:  # without filter, the log will be too long
    #     logging.info(json.dumps({**stats, **domain_stats}))
    for csv_row in urls_and_instructions:
        yield csv_row


def main():
    # This is for the downloaded WARC files if they are stored in local device.
    # If the downloaded WARC files are stored in your own remote file system,
    # please costomize this part.

    segments = ['1627046152156.49', '1627046153531.10', '1627046153860.57', '1627046154032.75']
    warc_file = wget.download(PATHS_FILE)
    with gzip.open(warc_file, 'rb') as f_in:
        # Ignore .gz extention
        with open(warc_file[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    segment_dict = collections.defaultdict(list)
    with open(warc_file[:-3], 'r') as warc_paths:
        for warc_line in warc_paths.readlines():
            for segment in segments:
                if segment in warc_line.rstrip():
                    segment_dict[segment].append(warc_line.rstrip())
                    break

    os.remove(warc_file)
    os.remove(warc_file[:-3])

    # Taking 2 from each segment
    for segment_k in segment_dict:
        segment_dict[segment_k] = random.sample(segment_dict[segment_k], 2)

    csv_file_descriptor = open("dataset4_task.csv", 'a', newline='')
    csv_writer = csv.writer(csv_file_descriptor)
    csv_writer.writerow(HEADER)
    count = 0
    try:
        for segment in segment_dict:
            for warc_file_download_link in segment_dict[segment]:
                warc_file = wget.download(DOWNLOAD_PREFIX + warc_file_download_link)
                download_dir = segment + "_" + warc_file.split(".")[0].split("-")[-1]
                os.makedirs(download_dir)
                with open(warc_file, 'rb') as f1:
                    with gzip.open(f1, mode='rt', encoding='latin1') as f2:
                        for csv_row in extract_instructions_from_warc_file(warc_file, f2, download_dir=download_dir):
                            csv_writer.writerow(csv_row)
                            count += 1
                            if count >= 100:
                                return
                os.remove(warc_file)

    finally:
        os.remove(warc_file)
        csv_file_descriptor.close()


if __name__ == '__main__':
    # FLAGS.set_default('logtostderr', True)
    # app.run(main)
    main()
