from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import PIL.Image
from PIL.ExifTags import TAGS
import exif
import requests,pdfx
from pprint import pprint
import pandas as pd
import io, ssl

ssl._create_default_https_context = ssl._create_unverified_context
def retrieve_url_image(url):
    req = Request(url , headers={'User-Agent': 'Mozilla/5.0'})
    page = urlopen(req).read()
    builder = BeautifulSoup(page, "lxml")
    images_url = []
    for img_url in builder.find_all('img'):
        print(img_url)
        if img_url.get('src') or img_url.get('data-src') is not None:
            print(img_url.get('src'))
            if img_url.get('src') is not None:
                if img_url.get('src').startswith("http"):
                    spliturl = img_url.get('src').split("?",1)
                    url = spliturl[0]
                    if spliturl[0].endswith("svg"):
                        continue
                    else:
                        images_url.append(url)
                else:
                    continue
            elif img_url.get('data-src'):
                if img_url.get('data-src').startswith("http"):
                    spliturl = img_url.get('data-src').split("?",1)
                    url = spliturl[0]
                    if spliturl[0].endswith("svg"):
                        continue
                    else:
                        images_url.append(url)
                else:
                    continue
            else:
                continue
        else:
            continue
    return images_url

def retrieve_metadata(url_retrieved, url_add):
    df_metadataImg = pd.DataFrame()
    listMetadata = []
    for i in range(len(url_retrieved)):
        print(f"======= < {i+1} > URL image of {len(url_retrieved)} Totals URL image =======")
        list_data = []
        index_tag = []
        try:
            req = requests.get(url_retrieved[i], stream=True)
            img = PIL.Image.open(io.BytesIO(req.content))
            exifdata = img.getexif()
            for tagid in exifdata:
                if tagid in TAGS.keys():
                    tagname = TAGS.get(tagid, tagid)
                    index_tag.append(tagname)
                    value = exifdata.get(tagid)
                    list_data.append(value)
                else:
                    pass
        except:
            pass
        
        df_metadataImg = pd.DataFrame(list_data, index=index_tag, columns=[url_retrieved[i]])
        if not df_metadataImg.empty:
            listMetadata.append(df_metadataImg)
            pprint(listMetadata)
        else:
            print("skip")
            continue
    return listMetadata

def retrieve_url_file(url):
    req = Request(url , headers={'User-Agent': 'Mozilla/5.0'})
    page = urlopen(req).read()
    builder = BeautifulSoup(page, "html.parser")
    files_url = []
    tag_a = builder.find_all('a')

    for link in tag_a:
        href_link = link.get('href')
        if href_link is not None:
            if href_link.endswith('pdf'):
                files_url.append(href_link)
            else:
                continue
        else:
            continue
    return files_url

def retrieve_file_metadata(url_file, url_add):
    df_metadataFile = []
    listMetadataFile = []
    
    for i in range(len(url_file)):
        print(f"======= < {i+1} > URL File of {len(url_file)} Totals URL file =======")
        list_data = []
        index_tag = []
        try:
            print("not in")
            file = pdfx.PDFx(url_file[i])
            metadata = file.get_metadata()
            for tagid in metadata:
                index_tag.append(tagid)
                value = metadata.get(tagid)
                list_data.append(value)
        except:
            continue
        
        df_metadataFile = pd.DataFrame(list_data, columns=[url_file[i]], index=index_tag)
        if not df_metadataFile.empty:
                listMetadataFile.append(df_metadataFile)
                pprint(df_metadataFile)
        else:
            print("skip")
            continue

    return listMetadataFile
        # txt = f"""
        #     \nMETADATA USING PYPDF\n
        #     Author: {information.author}
        #     Creator: {information.creator}
        #     Title: {information.title}
        #     author = info.author
        #     creator = info.creator
        #     producer = info.producer
        #     subject = info.subject
        #     title = info.title
        # """