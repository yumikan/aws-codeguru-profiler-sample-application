import csv
import boto3

# Client and session information
session = boto3.Session()
s3_client = session.client(service_name="s3")
comprehend_client = session.client(service_name="comprehend")
translate_client = session.client(service_name="translate")

with open('input.csv') as f:
    reader = csv.reader(f)
    list_in = [row for row in reader]

output_list = []

first_row = ["コメント", "Check", "文字数", "挨拶文", "Neutral", "Positive", "Negative", "Mixed", "Translated", "Neutral", "Positive", "Negative", "Mixed", "日本語", "Entity_PayPay", "Entity_パーセンテージ", "Entities", "Key Phrases"]
output_list.append(first_row)

comment=""
i=0

while i < len(list_in):
    list = []
    temp=list_in[i][0]

    #コメントが""で囲まれていて複数行に渡るものがあったため、同じコメントかどうか判定
    if len(temp)>1 and temp[1]=="\"" and len(comment)==0:
        print("New sentence starting with double quote. i is ", i)
        comment+=temp
        i+=1
    elif len(comment)>0 and temp[-1]!="\"":
        print("a sentence has not ended yet. i is ", i)
        comment+=temp
        i+=1
    else:
        print("Sentence ended. Ready to call APIs. i is ", i)

        comment+=temp
        list.append(comment)

        #Initialize check flag
        check=0

        #Perform character count check
        length=len(comment)
        if length < 10:
            check=1

        #Perform check on certain keywords
        common_words=0
        if 'お世話に' in comment or 'ありがとうございます' in comment or '利用して' in comment:
            check=1
            common_words=1

        #Call Comprehend for sentiment analysis, Japanese
        response = comprehend_client.detect_sentiment(Text=comment, LanguageCode="ja")
        Positive_ja=response["SentimentScore"]["Positive"]
        Negative_ja=response["SentimentScore"]["Negative"]
        Neutral_ja=response["SentimentScore"]["Neutral"]
        Mixed_ja=response["SentimentScore"]["Mixed"]
        positive_ja='{:.3f}'.format(Positive_ja)
        negative_ja='{:.3f}'.format(Negative_ja)
        neutral_ja='{:.3f}'.format(Neutral_ja)
        mixed_ja='{:.3f}'.format(Mixed_ja)

        if float(negative_ja) > 0.8 or float(mixed_ja) > 0.8:
            check=1

        #Call Translate for translation
        response_en = translate_client.translate_text(
            Text=comment,
            SourceLanguageCode='ja',
            TargetLanguageCode='en'
        )

        comment_en=response_en["TranslatedText"]

        #Call Comprehend for sentiment analysis, English
        response = comprehend_client.detect_sentiment(Text=comment_en, LanguageCode="en")
        Positive_en=response["SentimentScore"]["Positive"]
        Negative_en=response["SentimentScore"]["Negative"]
        Neutral_en=response["SentimentScore"]["Neutral"]
        Mixed_en=response["SentimentScore"]["Mixed"]
        positive_en='{:.3f}'.format(Positive_en)
        negative_en='{:.3f}'.format(Negative_en)
        neutral_en='{:.3f}'.format(Neutral_en)
        mixed_en='{:.3f}'.format(Mixed_en)

        #Call Comprehend for dominant language detection
        response = comprehend_client.detect_dominant_language(Text=comment)
        languages=response["Languages"]
        j=0
        Ja_score=0
        while j < len(languages):
            if languages[j]["LanguageCode"] != "ja":
                j+=1
            else:
                Ja_score = languages[j]["Score"]
                j = len(languages)

        if Ja_score < 0.8:
            check=1

        ja_score = '{:.3f}'.format(Ja_score)

        #Call Comprehend for entity detection, Japanese
        response = comprehend_client.detect_entities(Text=comment, LanguageCode="ja")
        entities = response["Entities"]

        k=0
        PayPay_detected = 0
        percentage_detected = 0

        entities_list=[]

        while k < len(entities):
            entity = entities[k]["Text"]
            if "PayPay" in entity or "paypay" in entity or "Paypay" in entity or "ペイペイ" in entity:
                PayPay_detected = entities[k]["Score"]
                check = 1
            elif "%" in entity:
                percentage_detected = entities[k]["Score"]
                check = 1
            entities_list.append(entity)
            k+=1

        #Call Comprehend for key phrases detection - added 20201216
        response = comprehend_client.detect_key_phrases(Text=comment, LanguageCode="ja")
        key_phrases = response["KeyPhrases"]

        l=0
        key_phrases_list=[]

        while l < len(key_phrases):
            key_phrase=key_phrases[l]["Text"]
            key_phrases_list.append(key_phrase)
            l+=1

        #Append results to list
        list.append(check)
        list.append(length)
        list.append(common_words)
        list.append(neutral_ja)
        list.append(positive_ja)
        list.append(negative_ja)
        list.append(mixed_ja)
        list.append(comment_en)
        list.append(neutral_en)
        list.append(positive_en)
        list.append(negative_en)
        list.append(mixed_en)
        list.append(ja_score)
        list.append(PayPay_detected)
        list.append(percentage_detected)
        list.append(entities_list)
        list.append(key_phrases_list)

        #Append to list for csv
        output_list.append(list)

        i+=1
        comment=""

with open("output.csv", "w") as f:
    writer = csv.writer(f, lineterminator='\n')
    writer.writerows(output_list)
