## Image captioning dataset creation

# CSE 576 Natural Language processing (Project I)

Use common crawl archive to download warc files:
https://commoncrawl.org/2021/08/july-august-2021-crawl-archive-available/ 

Segments choosen here: 627046152156.49 1627046153531.10 1627046153860.57 1627046154032.75
(The csv in this repo has crawled 5, 5, 2, 2 warc files fromt this segments respectively.)

CSV Header format:
UUID, Image-URL, Image-Local-Path, Image-Alt-Text-Caption, A Context Paragraph within the HTML which Refers the Image, Segment, WARCFile, Source WebPage URL.

Crawler condition:
* Image should have atleast 400x400 size
* NSFW Domain blacklisting using https://github.com/olbat/ut1-blacklists
* Using nudeNet(https://github.com/notAI-tech/NudeNet) to filter out NSFW images

Further Filtering:
* Filtering out non unicode characters from alt_text and context of the dataset.
* Language check to make sure the alt_text and context belongs any unicode language. (Used langdetect python module)
* Contexual comparisation of alt_text and context sections for each data entry using consine similary of embeddings from SentenceTransformer(bert-base-nli-mean-tokens embeddings). Only keeping top 5 sentences in context text.

# Filteration can be further expanded using feature extration on the image


