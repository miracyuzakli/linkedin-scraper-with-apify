import streamlit as st
from apify_client import ApifyClient
import pandas as pd
import os
import json
import time
import io
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_profile_or_company(url):
    urlx = url.strip('/').split('/')[-2]
    if urlx == 'company':
        return 'company'
    else:
        return 'profile'

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
    organization_data = {}
    for entry in data:
        city = entry.get("headquarter", {}).get("city", "")
        line1 = entry.get("headquarter", {}).get("line1", "")
        line2 = entry.get("headquarter", {}).get("line2", "")
        city = f'{city} {line1 if line1 else ""} {line2 if line2 else ""}'.strip()
        industries = ", ".join(
            [industry.get("name", "") for industry in entry.get("industries", [])]
        )
        organization = {
            "name": entry.get("name"),
            "description": entry.get("description"),
            "tagline": entry.get("tagline"),
            "websiteUrl": entry.get("websiteUrl"),
            "followerCount": entry.get("followerCount"),
            "country": entry.get("headquarter", {}).get("country"),
            "geographicArea": entry.get("headquarter", {}).get("geographicArea"),
            "city": city,
            "postalCode": entry.get("headquarter", {}).get("postalCode"),
            "industries": industries,
        }
        organization_data.update(organization)
    return organization_data


def clean_json_data_for_profile(data):
    if not data:
        return {}
    profile = data[0]
    # Process the profile data as before
    positions = profile.get("positions", [])
    current_position = (
        f'{positions[0]["title"]} at {positions[0]["companyName"]}'
        if positions
        else "N/A"
    )
    certifications = ", ".join(
        [cert.get("name", "") for cert in profile.get("certifications", [])]
    )
    languages = ", ".join(
        [
            f'{lang.get("name", "")} ({lang.get("proficiency", "")})'
            for lang in profile.get("languages", [])
        ]
    )
    profile_entry = {
        "name": f'{profile.get("firstName", "")} {profile.get("lastName", "")}'.strip(),
        "occupation": profile.get("occupation", ""),
        "headline": profile.get("headline", ""),
        "summary": profile.get("summary", ""),
        "skills": ", ".join(profile.get("skills", [])),
        "followersCount": profile.get("followersCount", 0),
        "connectionsCount": profile.get("connectionsCount", 0),
        "profilePicture": profile.get("pictureUrl", ""),
        "linkedinUrl": f'https://www.linkedin.com/in/{profile.get("publicIdentifier", "")}',
        "currentPosition": current_position,
        "certifications": certifications,
        "languages": languages,
        "geoLocation": profile.get("geoLocationName", ""),
        "geoCountry": profile.get("geoCountryName", ""),
    }
    return profile_entry


def get_post_data(url, cookie):
    client = ApifyClient(os.getenv("APIFY_TOKEN"))
    run_input = {
        "cookie": cookie,
        "deepScrape": False,
        "maxDelay": 5,
        "minDelay": 2,
        "limitPerSource": 5,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyGroups": [],
            "apifyProxyCountry": "US"
        },
        "rawData": False,
        "urls": [url]
    }
    run = client.actor("kfiWbq3boy3dWKbiL").call(run_input=run_input)
    post_data = list()
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        post_data.append(item)
    return post_data

def get_company_data(url, cookie):
    client = ApifyClient(os.getenv("APIFY_TOKEN"))
    run_input = {
        "cookie": cookie,
        "maxDelay": 5,
        "minDelay": 2,
        "urls": [url]
    }
    run = client.actor("CzfgYQcC57pYWSXbv").call(run_input=run_input)
    company_data = list()
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        company_data.append(item)
    return company_data


def get_profile_data(url, cokie):
    client = ApifyClient(os.getenv("APIFY_TOKEN"))

    run_input = {
        "cookie": cokie,
        "minDelay": 5,
        "maxDelay": 30,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyCountry": "US",
        },
        "urls": [url]
    }

    # Run the Actor and wait for it to finish
    run = client.actor("PEgClm7RgRD7YO94b").call(run_input=run_input)

    profile_data = list()
    # Fetch and print Actor results from the run's dataset (if there are any)
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        profile_data.append(item)

    return profile_data

# Streamlit App
st.title('LinkedIn Company Scraper')

st.write('Please ensure that your APIFY_TOKEN is set in the .env file.')

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

        # Parse the cookie JSON
        try:
            cookie = json.loads(cookie_json_str)
        except json.JSONDecodeError as e:
            st.error(f'Error parsing cookie JSON: {str(e)}')
            st.stop()

        data_list = []
        total_urls = len(urls)
        progress_bar = st.progress(0)
        status_text = st.empty()

        for idx, url in enumerate(urls, start=1):
            start_time = time.time()
            current_progress = idx / total_urls
            progress_bar.progress(current_progress)
            status_text.text(f'Processing URL {idx}/{total_urls}: {url}')

            temp_data = {'URL': url}

            try:
                url_type = get_profile_or_company(url)
                if url_type == 'company':
                    # Company data
                    temp_company_data = get_company_data(url, cookie)
                    cleaned_company_data = clean_json_data_for_company(temp_company_data)

                    # Posts data
                    temp_post_data = get_post_data(url, cookie)
                    cleaned_post_data = clean_json_data_for_posts(temp_post_data)

                    temp_data.update(cleaned_company_data)
                    temp_data.update(cleaned_post_data)
                else:
                    
                    # Profile data
                    temp_profile_data = get_profile_data(url, cookie)
                    cleaned_profile_data = clean_json_data_for_profile(temp_profile_data)

                    # Posts data
                    temp_post_data = get_post_data(url, cookie)
                    cleaned_post_data = clean_json_data_for_posts(temp_post_data)

                    temp_data.update(cleaned_profile_data)
                    temp_data.update(cleaned_post_data)



            except Exception as e:
                st.error(f'Error processing URL {idx}: {url}. Error message: {str(e)}')
                continue

            end_time = time.time()
            run_time = end_time - start_time
            st.write(f'Finished processing URL {idx}/{total_urls}: {url} in {run_time:.2f} seconds')

            data_list.append(temp_data)

        if data_list:
            result_df = pd.DataFrame(data_list)
            st.write('Scraping completed.')
            st.dataframe(result_df)

            # Provide download link
            def convert_df(df):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                processed_data = output.getvalue()
                return processed_data

            excel_data = convert_df(result_df)
            st.download_button(label='Download data as Excel', data=excel_data, file_name='output.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            st.warning('No data to display.')
