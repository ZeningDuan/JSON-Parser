#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Full Twitter API 2.0 parser written by Luhang Sun in June 2022
#Updated by Zening DUAN, June 2022


import os
import pandas as pd
import numpy as np
import json
import twarc #twarc-2.14.0
import glob



#Add RT section to the full texts in the dataframe
def add_full_text(df):
    full_text = []
    for n in range(len(df)):
        if "RT @" in df['text'][n]:
            if type(df['text'][n]) == str and type(df['referenced_tweets.text'][n]) == str:
                full = df['text'][n].partition(":")[0] + ": " + df['referenced_tweets.text'][n]
            else:
                full = df['text'][n]
        else:
            full = df['text'][n]
        full_text.append(full)
        n = n + 1
    df['text_full'] = full_text
    df['text_full'] = df['text_full'].replace('\n', '.',regex=True)
    
    return df

#Extract non-truncated retweet texts from "includes":
#aka expansions fields of "tweets" in "includes" in the raw JSON files
def extract_referenced_tweets(df):
    df_rt = df['referenced_tweets']
    
#Create empty lists for non-nested fields: 
#text, type, id, author_id, possibly_sensitive, conversation_id, created_at, lang, source, reply_settings.
    referenced_tweets_text = []         #1. create an empty list for non-truncated "text" in referenced_tweets
    referenced_tweets_type = []         #2. create an empty list for "type" in referenced_tweets
    referenced_tweets_id = []           #3. create an empty list for "id" in referenced_tweets
    referenced_tweets_author_id = []    #4. create an empty list for "author_id" in referenced_tweets
    referenced_tweets_sensitive = []    #5. create an empty list for "possibly_sensitive" in referenced_tweets
    referenced_tweets_conversation = [] #6. create an empty list for "conversation_id" in referenced_tweets
    referenced_tweets_created_at = []   #7. create an empty list for "created_at" in referenced_tweets
    referenced_tweets_lang = []         #8. create an empty list for "lang" in referenced_tweets
    referenced_tweets_source = []       #9. create an empty list for "source" in referenced_tweets
    referenced_tweets_reply = []        #10. create an empty list for "reply_settings" in referenced_tweets

#Nested fields: entities, public_metrics, attachments, author
#The current version won't parse all the content within the nested fields
#Because they are information of the original referenced tweets
    referenced_tweets_entities = []        #11. create an empty list for "entities" in referenced_tweets
    referenced_tweets_public_metrics = []  #12. create an empty list for "public_metrics" in referenced_tweets
    referenced_tweets_attachments = []     #13. create an empty list for "attachments" in referenced_tweets
    referenced_tweets_author = []          #14. create an empty list for "authors" in referenced_tweets
    
# #Nested fields: entities, public_metrics, attachments, author
#     referenced_tweets_entities_urls_start = []        #11. create an empty list for "entities"-"urls"-"start" in referenced_tweets
#     referenced_tweets_entities_urls_end = []          #12. create an empty list for "entities"-"urls"-"end" in referenced_tweets
#     referenced_tweets_entities_urls_url = []          #13. create an empty list for "entities"-"urls"-"url" in referenced_tweets
#     referenced_tweets_entities_urls_expanded_url = [] #14. create an empty list for "entities"-"urls"-"expanded_url" in referenced_tweets
#     referenced_tweets_entities_urls_display_url = []  #15. create an empty list for "entities"-"urls"-"display_url" in referenced_tweets
#     referenced_tweets_entities_hashtags_start = []    #16. create an empty list for "entities"-"hashtags"-"start" in referenced_tweets
#     referenced_tweets_entities_hashtags_end = []      #17. create an empty list for "entities"-"hashtags"-"end" in referenced_tweets
#     referenced_tweets_entities_hashtags_tag = []      #18. create an empty list for "entities"-"hashtags"-"tag" in referenced_tweets
#     referenced_tweets_public_metrics_retweet = []     #19. create an empty list for "public_metrics"-"retweet_count" in referenced_tweets
#     referenced_tweets_public_metrics_reply = []       #20. create an empty list for "public_metrics"-"reply_count" in referenced_tweets
#     referenced_tweets_public_metrics_like = []        #21. create an empty list for "public_metrics"-"like_count" in referenced_tweets
#     referenced_tweets_public_metrics_quote = []       #22. create an empty list for "public_metrics"-"quote_count" in referenced_tweets

    for i in df_rt:
        if type(i) == list: #if this cell is not NaN, its type will be "list"
            #1. extract "text" from referenced_tweets which could be retweeted, replied_to, or quoted tweets
            ls_text = [d['text'] for d in i if 'text' in d]
            str_text = ' '.join(map(str, ls_text)) #make the referenced texts "string"
            #2. extract "type" from referenced_tweets which could be retweeted, replied_to, or quoted
            ls_type = [d['type'] for d in i if 'type' in d]
            str_type = ' '.join(map(str, ls_type)) #make the referenced type "string" 
            #3. extract "id" from referenced_tweets
            ls_id = [d['id'] for d in i if 'id' in d]
            str_id = ' '.join(map(str, ls_id)) #make the referenced id "string"
            #4. extract "author_id" from referenced_tweets
            ls_author_id = [d['author_id'] for d in i if 'author_id' in d]
            str_author_id = ' '.join(map(str, ls_author_id)) #make the referenced author_id "string"
            #5. extract "possibly_sensitive" from referenced_tweets
            ls_sensitive = [d['possibly_sensitive'] for d in i if 'possibly_sensitive' in d]
            str_sensitive = ' '.join(map(str, ls_sensitive)) #make the referenced possibly_sensitive "string"           
            #6. extract "conversation_id" from referenced_tweets
            ls_conversation = [d['conversation_id'] for d in i if 'conversation_id' in d]
            str_conversation = ' '.join(map(str, ls_conversation)) #make the referenced conversation_id "string"             
            #7. extract "created_at" from referenced_tweets
            ls_conversation = [d['conversation_id'] for d in i if 'conversation_id' in d]
            str_conversation = ' '.join(map(str, ls_conversation)) #make the referenced conversation_id "string"             
            #8. extract "lang" from referenced_tweets
            ls_lang = [d['lang'] for d in i if 'lang' in d]
            str_lang = ' '.join(map(str, ls_lang)) #make the referenced lang "string"   
            #9. extract "source" from referenced_tweets
            ls_source = [d['source'] for d in i if 'source' in d]
            str_source = ' '.join(map(str, ls_source)) #make the referenced source "string"   
            #10. extract "reply_settings" from referenced_tweets
            ls_reply = [d['reply_settings'] for d in i if 'reply_settings' in d]
            str_reply = ' '.join(map(str, ls_reply)) #make the referenced reply_settings "string"   
            
            #Nested fields: entities, public_metrics, attachments, author
            #11. extract "entities" from referenced_tweets
            ls_entities = [d['entities'] for d in i if 'entities' in d]
            str_entities = ' '.join(map(str, ls_entities))            
            #12. extract "public_metrics" from referenced_tweets
            ls_metrics = [d['public_metrics'] for d in i if 'public_metrics' in d]
            str_metrics = ' '.join(map(str, ls_metrics))            
            #13. extract "attachments" from referenced_tweets
            ls_attachments = [d['attachments'] for d in i if 'attachments' in d]
            str_attachments = ' '.join(map(str, ls_attachments))            
            #14. extract "author" from referenced_tweets
            ls_author = [d['author'] for d in i if 'author' in d]
            str_author = ' '.join(map(str, ls_author))            
   
        else: #if this cell is NaN, it is an original tweet
            str_text = np.nan
            str_type = 'original'
            str_id = np.nan
            str_author_id = np.nan
            str_sensitive = np.nan
            str_conversation = np.nan
            str_lang = np.nan
            str_source = np.nan
            str_reply = np.nan
            #Nested fields:
            str_entities = np.nan
            str_metrics = np.nan
            str_attachments = np.nan
            str_author = np.nan

            
        referenced_tweets_text.append(str_text)
        referenced_tweets_type.append(str_type)
        referenced_tweets_id.append(str_id)
        referenced_tweets_author_id.append(str_author_id)
        referenced_tweets_sensitive.append(str_sensitive)
        referenced_tweets_conversation.append(str_conversation)
        referenced_tweets_lang.append(str_lang)
        referenced_tweets_source.append(str_source)
        referenced_tweets_reply.append(str_reply)
        #Nested fields:
        referenced_tweets_entities.append(str_entities)
        referenced_tweets_public_metrics.append(str_metrics)
        referenced_tweets_attachments.append(str_attachments) 
        referenced_tweets_author.append(str_author)

    df['referenced_tweets.text'] = referenced_tweets_text
    df['referenced_tweets.text'] = df['referenced_tweets.text'].replace('\n', '.',regex=True)
    df['referenced_tweets.type'] = referenced_tweets_type
    df['referenced_tweets.id'] = referenced_tweets_id
    df['referenced_tweets.author_id'] = referenced_tweets_author_id
    df['referenced_tweets.possibly_sensitive'] = referenced_tweets_sensitive
    df['referenced_tweets.conversation_id'] = referenced_tweets_conversation
    df['referenced_tweets.lang'] = referenced_tweets_lang
    df['referenced_tweets.source'] = referenced_tweets_source
    df['referenced_tweets.reply_settings'] = referenced_tweets_reply
    df['referenced_tweets.entities'] = referenced_tweets_entities
    df['referenced_tweets.public_metrics'] = referenced_tweets_public_metrics
    df['referenced_tweets.attachments'] = referenced_tweets_attachments
    df['referenced_tweets.author'] = referenced_tweets_author

    df = add_full_text(df)
    return df


def extract_expanded_urls(df):
    url_list = df['entities.urls']

#Create a column of expanded_url in df:
    expanded_url = []
    for i in url_list:
        if type(i) == list:
            ls_expanded_url = [d['expanded_url'] for d in i if 'expanded_url' in d]
            str_ls_expanded_url = ' '.join(map(str, ls_expanded_url))
        else:
            str_ls_expanded_url = np.nan
        expanded_url.append(str_ls_expanded_url)
    df["entities.urls.expanded_url"] = expanded_url
    
#Count the total number of expanded_url:
    counts_expanded_url = []
    for n in expanded_url:
        if type(n) == str:
            expanded_url_count = n.count(' ') + 1
        else:
            expanded_url_count = np.nan
        counts_expanded_url.append(expanded_url_count)
    df["entities.urls.expanded_url_total_number"] = counts_expanded_url
    
    return df

def extract_media_urls(df):
#Extract media type first:
    attachments_media_type = []
    for i in df["attachments.media"]:
        if type(i) == list:
            ls_media_type = [d['type'] for d in i if 'type' in d]
            str_media_type = ' '.join(map(str, ls_media_type))
        else:
            str_media_type = np.nan
        attachments_media_type.append(str_media_type)
    df["attachments.media.type"] = attachments_media_type
#Extract media urls:
    attachments_media_urls = []
    for i in df["attachments.media"]:
        if type(i) == list:
            for d in i:
                if 'url' in d:
                    if 'preview_image_url' not in d:
                        ls_media_url = [d['url'] for d in i if 'url' in d]
                    if 'preview_image_url' in d:
                        ls_media_url = [d['url'] for d in i if 'url' in d] + ' ' + [d['preview_image_url'] for d in i if 'preview_image_url' in d]
                if 'preview_image_url' in d:
                    if 'url' not in d:
                        ls_media_url = [d['preview_image_url'] for d in i if 'preview_image_url' in d]
                    if 'url' in d:
                        ls_media_url = [d['url'] for d in i if 'url' in d] + ' ' + [d['preview_image_url'] for d in i if 'preview_image_url' in d]
                str_media_url = ' '.join(map(str, ls_media_url))
        else:
            str_media_url = 0
        attachments_media_urls.append(str_media_url)
    df["attachments.media.url"] = attachments_media_urls

#Count the media number of expanded_url:
    counts_media_url = []
    for n in attachments_media_urls:
        if type(n) == str:
            media_url_count = n.count(' ') + 1
        else:
            media_url_count = 0
        counts_media_url.append(media_url_count)
    df["attachments.media.url_total_number"] = counts_media_url

#Count the non-media number of expanded_url:
    df['textual_url_number'] = df["entities.urls.expanded_url_total_number"] - df["attachments.media.url_total_number"]
    return df


if __name__ == "__main__":
    main()




