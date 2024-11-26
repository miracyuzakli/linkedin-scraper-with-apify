import streamlit as st
from apify_client import ApifyClient
import pandas as pd
import os
import json
import time
import io
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Helper functions
def get_profile_or_company(url):
    urlx = url.strip('/').split('/')[-2]
    return 'company' if urlx == 'company' else 'profile'

def clean_json_data_for_posts(data):
    transformed_data = {}
    for i, post in enumerate(data, start=1):
        transformed_data[f"POST{i}URL"] = post.get("url", "")
        transformed_data[f"POST{i}TEXT"] = post.get("text", "")
        transformed_data[f"POST{i}LIKE"] = post.get("numLikes", 0)
        transformed_data[f"POST{i}COMMENT"] = post.get("numComments", 0)
        transformed_data[f"POST{i}SHARE"] = post.get("numShares", 0)
    return transformed_data

def clean_json_data_for_company(data):
    if not data:
        return {}
    entry = data[0]
    city = entry.get("headquarter", {}).get("city", "")
    line1 = entry.get("headquarter", {}).get("line1", "")
    line2 = entry.get("headquarter", {}).get("line2", "")
    city_full = f'{city} {line1 if line1 else ""} {line2 if line2 else ""}'.strip()
    industries = ", ".join([industry.get("name", "") for industry in entry.get("industries", [])])
    return {
        "name": entry.get("name"),
        "occupation": entry.get("tagline", ""),
        "headline": entry.get("description", ""),
        "summary": entry.get("description", ""),
        "skills": industries,
        "followerCount": entry.get("followerCount"),
        "connectionsCount": None,
        "profilePicture": entry.get("image", ""),
        "linkedinUrl": entry.get("url"),
        "currentPosition": None,
        "certifications": None,
        "languages": None,
        "geoLocation": city_full,
        "geoCountry": entry.get("headquarter", {}).get("country"),
        "websiteUrl": entry.get("websiteUrl"),
    }

def clean_json_data_for_profile(data):
    if not data:
        return {}
    profile = data[0]
    positions = profile.get("positions", [])
    current_position = f'{positions[0]["title"]} at {positions[0]["companyName"]}' if positions else ""
    certifications = ", ".join([cert.get("name", "") for cert in profile.get("certifications", [])])
    languages = ", ".join([f'{lang.get("name", "")} ({lang.get("proficiency", "")})' for lang in profile.get("languages", [])])
    return {
        "name": f'{profile.get("firstName", "")} {profile.get("lastName", "")}'.strip(),
        "occupation": profile.get("occupation", ""),
        "headline": profile.get("headline", ""),
        "summary": profile.get("summary", ""),
        "skills": ", ".join(profile.get("skills", [])),
        "followerCount": profile.get("followersCount", 0),
        "connectionsCount": profile.get("connectionsCount", 0),
        "profilePicture": profile.get("pictureUrl", ""),
        "linkedinUrl": f'https://www.linkedin.com/in/{profile.get("publicIdentifier", "")}',
        "currentPosition": current_position,
        "certifications": certifications,
        "languages": languages,
        "geoLocation": profile.get("geoLocationName", ""),
        "geoCountry": profile.get("geoCountryName", ""),
        "websiteUrl": None,
    }

def get_post_data(url, cookie):
    client = ApifyClient(os.getenv("APIFY_TOKEN"))
    run_input = {
        "cookie": cookie,
        "deepScrape": False,
        "maxDelay": 5,
        "minDelay": 2,
        "limitPerSource": 5,
        "proxy": {"useApifyProxy": True, "apifyProxyCountry": "US"},
        "rawData": False,
        "urls": [url]
    }
    run = client.actor("kfiWbq3boy3dWKbiL").call(run_input=run_input)
    return list(client.dataset(run["defaultDatasetId"]).iterate_items())

def get_company_data(url, cookie):
    client = ApifyClient(os.getenv("APIFY_TOKEN"))
    run_input = {
        "cookie": cookie,
        "maxDelay": 5,
        "minDelay": 2,
        "urls": [url]
    }
    run = client.actor("CzfgYQcC57pYWSXbv").call(run_input=run_input)
    return list(client.dataset(run["defaultDatasetId"]).iterate_items())

def get_profile_data(url, cookie):
    client = ApifyClient(os.getenv("APIFY_TOKEN"))
    run_input = {
        "cookie": cookie,
        "minDelay": 5,
        "maxDelay": 30,
        "proxy": {"useApifyProxy": True, "apifyProxyCountry": "US"},
        "urls": [url]
    }
    run = client.actor("PEgClm7RgRD7YO94b").call(run_input=run_input)
    return list(client.dataset(run["defaultDatasetId"]).iterate_items())

# Async wrapper for functions
async def async_run_function(func, *args):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, func, *args)

async def process_url_async(url, cookie):
    url_type = get_profile_or_company(url)
    temp_data = {'URL': url}
    if url_type == 'company':
        company_task = async_run_function(get_company_data, url, cookie)
        post_task = async_run_function(get_post_data, url, cookie)
        company_data, post_data = await asyncio.gather(company_task, post_task)
        temp_data.update(clean_json_data_for_company(company_data))
        temp_data.update(clean_json_data_for_posts(post_data))
    else:
        profile_task = async_run_function(get_profile_data, url, cookie)
        post_task = async_run_function(get_post_data, url, cookie)
        profile_data, post_data = await asyncio.gather(profile_task, post_task)
        temp_data.update(clean_json_data_for_profile(profile_data))
        temp_data.update(clean_json_data_for_posts(post_data))
    return temp_data

async def process_urls_in_batches(urls, cookie, batch_size=10):
    data_list = []
    total_batches = (len(urls) + batch_size - 1) // batch_size
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = start_idx + batch_size
        batch = urls[start_idx:end_idx]
        results = await asyncio.gather(*(process_url_async(url, cookie) for url in batch))
        data_list.extend(results)
    return data_list

# Streamlit app
st.title('LinkedIn Company Scraper')

cookie_json_str = st.text_area('Enter your LinkedIn cookie JSON (as a list of dictionaries)')
uploaded_file = st.file_uploader('Upload an Excel file containing URLs', type='xlsx')

if st.button('Start'):
    if not os.getenv("APIFY_TOKEN"):
        st.error('APIFY_TOKEN not found. Please set it in your .env file.')
    elif not cookie_json_str:
        st.error('Please enter your cookie value.')
    elif not uploaded_file:
        st.error('Please upload an Excel file containing URLs.')
    else:
        try:
            data = pd.read_excel(uploaded_file)
            urls = data['URL'].tolist()
        except Exception as e:
            st.error(f'Error reading the uploaded file: {str(e)}')
            st.stop()

        try:
            cookie = json.loads(cookie_json_str)
        except json.JSONDecodeError as e:
            st.error(f'Error parsing cookie JSON: {str(e)}')
            st.stop()

        async def scrape_data():
            progress_bar = st.progress(0)
            status_text = st.empty()
            total_urls = len(urls)
            data_list = []
            for i in range(0, len(urls), 10):
                batch_urls = urls[i:i + 10]
                progress_bar.progress(i / total_urls)
                batch_results = await process_urls_in_batches(batch_urls, cookie, batch_size=10)
                data_list.extend(batch_results)
                status_text.text(f'Processed {min(i + 10, total_urls)} / {total_urls} URLs')
            return data_list

        data_list = asyncio.run(scrape_data())

        if data_list:
            result_df = pd.DataFrame(data_list)
            st.write('Scraping completed.')
            st.dataframe(result_df)

            def convert_df(df):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                return output.getvalue()

            excel_data = convert_df(result_df)
            st.download_button(label='Download data as Excel', data=excel_data, file_name='output.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            st.warning('No data to display.')
