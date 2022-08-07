# Web scraping from blogger site into database
# Company: HREF Tools Corp.
# Author: Manya Mutschler-Aldine
#
# This script gets the basic information for each article in the blogger site and 
# inserts it into the database and uploads the images to s3
# Info included:
#   - title, publication date, what3words for publication location, keywords
#   -  Stripped article html (the only attributes left on tags are src and href)
#       with the image (and associated href) tags changed to point to the s3 location of the images
#   - Author Ann and blogsite 1 (pulled from database)

# IMPORTS
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
import datetime
import re
import what3words
import boto3
import psycopg2
from psycopg2 import extras

# Constants
BUCKET_URL = os.environ["BUCKET_URL"]
IMAGE_FOLDER = os.environ["IMAGE_FOLDER"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
awsregion= os.environ["AWS_REGION"]
access_key= os.environ["ACCESS_KEY"]
secret_access_key = os.environ["SECRET_ACCESS_KEY"]

################################################################################
# Common utilities #############################################################

# Add broken image link and associated title to list
def addToBrokenImageLinks(imageAddress,articleTitle):
    brokenLinks.append(imageAddress+","+articleTitle)

# Upload image from given image address and filename to s3 bucket
def uploadImageToS3(imageAddress,filename):
    bucket = s3.Bucket(BUCKET_NAME)
    r = requests.get(imageAddress, stream=True)
    bucket.upload_fileobj(r.raw, IMAGE_FOLDER+filename)

################################################################################
# Scraping functions ###########################################################

# remove all attributes except some tags(only saving ['href','src'] attr)
def remove_all_attrs_except_saving(soupObj):
    whitelist = ['src','href']
    for tag in soupObj.find_all(True):
        if len(tag.find_all(True)) != 0:
            remove_all_attrs_except_saving(tag)
        attrs = dict(tag.attrs)
        for attr in attrs:
            if attr not in whitelist:
                del tag.attrs[attr]
    return soupObj

# Take in url, get title, date, location, keywords, html content, upload images to s3 and change references in html
def getArticleInfo(articleUrl):
    # Open and prepare article page
    try:
        page = urlopen(articleUrl)
        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
    except:
        return None

    # Get title
    title = next(soup.find('h3', {'class':'post-title entry-title'}).stripped_strings)

    # Get publication date
    date = soup.find_all('time',{'class':'published'})[0]['datetime']
    date = datetime.datetime.strptime(date[:10], '%Y-%m-%d')

    # Get keywords
    labelDiv = soup.find('div',{'class':'post-sidebar-item post-sidebar-labels'})
    keywords = []
    if labelDiv != None:
        for keyword in labelDiv.find_all('a',rel='tag'):
            keywords.append(keyword.string)

    # Get html article content
    contentDiv = soup.find('div',{'class':'post-body entry-content float-container'})

    # Find images and upload to s3, replace references in html
    images = contentDiv.find_all("img")
    for img in images:
        oldImageAddress = img['src']
        filename = re.search('[^/]+$',oldImageAddress).group(0)
        try:
            uploadImageToS3(oldImageAddress,filename)
            img['src']=BUCKET_URL+IMAGE_FOLDER+filename
            img['href']=BUCKET_URL+IMAGE_FOLDER+filename
            img.parent['href']=BUCKET_URL+IMAGE_FOLDER+filename
        except:
            addToBrokenImageLinks(oldImageAddress,title)
            continue

    # Clean html content
    contentClean = remove_all_attrs_except_saving(contentDiv)
    contentClean = contentClean.prettify()
    content = re.search('<div.*>((?s:.)*)</div>',str(contentDiv)).group(1)

    return {'title':title,'date':date,'keywords':keywords,'what3words':what3words,'content':content}

################################################################################
# Functions for putting article info in database ###############################

# Insert and link keyword for article
def insertKeyword(KW_Phrase,articleNo):
    cursor.execute('''SELECT KeywordNo FROM Keyword WHERE KW_Phrase=%s;''',(KW_Phrase,))
    keywordInfo = cursor.fetchone()
    if keywordInfo is None:
        cursor.execute('''INSERT INTO Keyword (KW_Phrase) VALUES(%s) RETURNING KeywordNo;''',(KW_Phrase,))
        conn.commit()
        keywordInfo = cursor.fetchone()
    keywordNo = keywordInfo['keywordno']

    cursor.execute('''INSERT INTO Keyword_Relation (KR_ArticleNo,KR_KeywordNo) VALUES(%s,%s);''',(articleNo,keywordNo))
    conn.commit()

# Insert article information
def insertArticleData(info,bloggerLink):
    cursor.execute('''INSERT INTO Article (AR_Title,AR_ContentHTML,AR_BlogsiteNo,AR_PublishedOnAt,
    AR_BloggerLink,AR_PublishedFrom) VALUES(%s,%s,%s,%s,%s,%s) RETURNING ArticleNo;''',(info['title'],info['content'],
        blogsiteNo,info['date'],bloggerLink,what3words))
    conn.commit()
    articleNo = cursor.fetchone()['articleno']
    # Insert Author connection
    cursor.execute('''INSERT INTO Author_Relation (UR_AuthorNo,UR_ArticleNo) VALUES(%s,%s);''',(authorNo,articleNo))
    conn.commit()
    # Insert keywords and link them
    for KW_Phrase in info['keywords']:
        insertKeyword(KW_Phrase,articleNo)

################################################################################
# Main #########################################################################

# Get all article information for each article and insert into database
def insertAllArticles():
    # Open and prepare main page
    mainUrl = 'http://needs-be.blogspot.com/'
    page = urlopen(mainUrl)
    html_bytes = page.read()
    html = html_bytes.decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")

    # Get list of archive links
    archiveLinksDiv = soup.find('div',{'id':'BlogArchive1_ArchiveList'})
    archiveLinks = archiveLinksDiv.find_all('a')

    # Go to each archive page
    for archiveLink in archiveLinks:
        # Open and prepare month archive page
        try:
            archiveUrl = archiveLink['href']
            archivePage = urlopen(archiveUrl)
            archiveHtml_bytes = archivePage.read()
            archiveHtml = archiveHtml_bytes.decode("utf-8")
            archiveSoup = BeautifulSoup(archiveHtml, "html.parser")
        except:
            print('could not open link: '+str(archiveLink))

        # Get each article link
        articles = archiveSoup.find_all('h3',{'class':'post-title entry-title'})
        for article in articles:
            articleLink = article.find('a')['href']
            # Get article data and insert into database
            info = getArticleInfo(articleLink)
            if info is None:
                continue
            insertArticleData(info,articleLink)

    return None

def main():
    # Connect to database
    conn = psycopg2.connect(
        host=os.enviorn["DB_HOST"],
        database=os.environ["DB"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"])
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # RealDictCursor means that DB selections are dicts rather than lists

    # Get Ann as Author
    cursor.execute('''SELECT AuthorNo FROM Author WHERE AU_Name='Ann Lynnworth';''')
    authorNo = cursor.fetchone()['authorno']
    # Get blogsite
    cursor.execute('''SELECT BlogsiteNo FROM Blogsite;''')
    blogsiteNo = cursor.fetchone()['blogsiteno']
    # Set location
    what3words = 'still.spices.swing'

    # Start list of broken links
    brokenLinks = []

    # Establish s3 connection
    s3 = boto3.resource('s3', region_name=awsregion, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

    # Insert articles into db
    insertAllArticles()

    # Write broken image links to a file
    with open('blogger_broken_image_links.csv','x') as brokenLinksFile:
        brokenLinksFile.write(",".join(brokenLinks))


# Run
if __name__=='__main__':
    main()
