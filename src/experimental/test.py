import requests
import xmltodict
from tools import save_json
def get_article_metadata(pubmed_id: str) -> dict:
    try:
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pubmed_id}"
        response = requests.get(url)
        xml_content = response.content
        body_response = xmltodict.parse(xml_content)
        return body_response

    except Exception as err:
        pass





if __name__=="__main__":
    metadata = get_article_metadata("PMC4174169")
    save_json(metadata, "test.json")