# Html cleanup of articles in database
# Company: HREF Tools Corp.
# Author: Manya Mutschler-Aldine
#
# This script cleans the article html for articles in the database (removes
# empty tags, unnecessary tag nesting etc.) and pulls previews from the text
# and inserts them into the database.

# Imports
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import extras

# Gets rid of all empty tags
def clean(tag):
    try:
        children = tag.contents
        deleted = True
        for child in children:
            deleted = deleted and clean(child)
        if deleted and tag.text.strip()=="" and tag.attrs=={}:
            tag.decompose()
            return True
        else:
            return False
    except:
        return tag.text.strip()==""

# Cleans article, extracts preview text, and updates database with both changes
def clean_article(article_soup, articleNo):
    # get rid of empty tags
    tags = article_soup.find_all(True)
    for tag in tags:
        clean(tag)

    # Change any h1 tags to h2
    h1tags = article_soup.find_all('h1')
    for h1tag in h1tags:
        h1tag.name='h2'

    # Get preview text
    preview_text = article_soup.get_text(strip=True)[:300]

    # Update in db
    cursor.execute('''UPDATE Article SET AR_ContentHTML=%s, AR_Preview=%s WHERE ArticleNo=%s''',(article_soup.prettify(),preview_text,articleNo))
    conn.commit()

def main():
    # Connect to database
    conn = psycopg2.connect(
        host=os.enviorn["DB_HOST"],
        database=os.environ["DB"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"])
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # RealDictCursor means that DB selections are dicts rather than lists

    # Get and clean all articles
    cursor.execute('''SELECT AR_ContentHTML, ArticleNo FROM Article''')
    articles = cursor.fetchall()
    for article in articles:
        article_soup = BeautifulSoup(article['ar_contenthtml'], "html.parser")
        clean_article(article_soup,article['articleno'])

if __name__ == '__main__':
    main()
