import time, re, requests, random
import pandas  as pd
import streamlit as st
from rich import print, print_json
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_filters_from_url(url):
    params_dict = {}
    experience_level_match = re.search(r'f_E=(.*?)&', url)
    job_function_match = re.search(r'f_F=(.*?)&', url)
    remote_options_match = re.search(r'f_WT=(.*?)&', url)
    
    if experience_level_match:
        params_dict["Experience Level"] = experience_level_match.group(1)
    if job_function_match:
        params_dict["Job Function"] = job_function_match.group(1)
    if remote_options_match:
        params_dict["Remote Options"] = remote_options_match.group(1)
    
    return params_dict

# def get_total_number_of_results(keyword, max_retries=2):
#     api_request_url = f"https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-67&count=25&q=jobSearch&query=(origin:SWITCH_SEARCH_VERTICAL,keywords:{keyword},spellCorrectionEnabled:true)&start=0"    
        
#     payload = {}
#     headers = {
#         'accept': 'application/vnd.linkedin.normalized+json+2.1',
#         'cookie': 'bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"; li_alerts=e30=; g_state={"i_l":0}; timezone=Europe/Stockholm; li_theme=light; li_theme_set=app; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; _gcl_au=1.1.308589430.1710419664; aam_uuid=16424388958969701103162659259461292262; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; visit=v=1&M; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AnalyticsSyncHistory=AQInqKM9VjeJfgAAAY6jDr95ykAKgdVEJ-lmi2hFEpuwpHs0GW_s9vj-G4Uw6j1j_pUJJhZMGdSj03dRsS-GKQ; lms_ads=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lms_analytics=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19817%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1712823944%7C6%7CMCAAMB-1712823944%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1712226344s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvqxsjRiMumDMdY2cZBC7rnBcwKqNM68r3TpZblRKHzhjqTvmVAWbcHGdsb5IwTFqJY%252fMUYh2Qg2S1xLvrOKsF819j5MizM%252fQkmqKNoUidY7bXjPqOzaXZfqS9qrp55bj79ludUr4VLcG1FqHXzI%252fnEZb6Gg8pzytrnrgQFlDD4qhZPoL773oMaOt5Xu7Zj6UYRpAMqFbr0QakvMVWMSvw93s%253d; li_g_recent_logout=v=1&true; lang=v=2&lang=en-us; li_at=AQEDASvMh7YD9s79AAABjqiLGqYAAAGOzJeepk0AeYx2DWyrkdJ2zOVnqqljd2pif0w70vXt5CAmfT-Fzviq450QuPbnNpN17uHRhNTjn38eeZfAzJg70FJChZAL8U0ElXl--_qooC9a45fdzqkaU7Sv; liap=true; JSESSIONID="ajax:2715582253737539260"; li_mc=MTsyMTsxNzEyMjI0NzMyOzI7MDIxzODtaxUnhH03NiFJxX2nAcg+Zt1SBwH5NvsRAxtAtmk=; UserMatchHistory=AQKpLFT71zoM5QAAAY6olBvzmoHGZBhPQlhG2QJfDL6VSRwwrqxGU5OYng_P7oC3i705LjK1mJLCoudXGg-J0NDW4inNM4LtM90f1IHjAKPkiKBQOsqx7x89ZsAUgQ_Id-tKl50XVNuPZnAfsIVhngEuxkV6538FxYjln7OKcc94E830eKTIGCzm9sUFevFdtaLpUziPshqg7A5qPAlpsBx_ltoEvRBdb6eZSTz-zYDAogKN9htKasaYbT-8BPcjbuJVhvAmT24k4rFh07c3Zx78yU0PaPxnN68ue8yS7BangTpKgFAr9JustG_rujNj9mHuB9A; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4190:u=253:x=1:i=1712225263:t=1712308566:v=2:sig=AQGNtWMcukq66YUQAxEHE2Y2woh1guTQ"; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19817%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1712823944%7C6%7CMCAAMB-1712823944%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1712226344s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; AnalyticsSyncHistory=AQInqKM9VjeJfgAAAY6jDr95ykAKgdVEJ-lmi2hFEpuwpHs0GW_s9vj-G4Uw6j1j_pUJJhZMGdSj03dRsS-GKQ; UserMatchHistory=AQKz6POJSiLt1AAAAY6okULilyFDXuLLHMAMYVzy-IMAs6Dlwno_fksOjnrsAnsZpD2MUiiNSG9oGzLrbQNa4N5CTJqA0FOGwUAH3-vZ9blAScHMZjWEElHwe_wJf4WbR02jFr8oZXirGt2T5fmAiHm_27xgkRrk0ivUr11nWHvdhh6l_QpEkhkJhkL1gItuDoH1ok95GHg4SC0rIoD7Txfw0C_QUZnpE8oMvyyScBkPIIwEBHuDwDKIW9Bd8LPkVpLt-FRLcxHxceXm1RjE12H6A3hq8Hmugcmg5htGzvIiW-lBiKnLsYGUSPowBkKdDtoFxVk; __cf_bm=46m5tvraQgrQpHhlW.Lwh9JA1WKE1fNfwR6JoACj1tc-1712225398-1.0.1.1-nXNAZAkaZHgAm2sBxkPt_tsSjFNf8oliPxZqefYXuc9oq6O6Lbwhyr7mIKovqCN1.OeYtBp7PvBct4AgSGNnUw; _gcl_au=1.1.308589430.1710419664; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; aam_uuid=16424388958969701103162659259461292262; bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvqxsjRiMumDMdY2cZBC7rnBcwKqNM68r3TpZblRKHzhjqTvmVAWbcHGdsb5IwTFqJY%252fMUYh2Qg2S1xLvrOKsF819j5MizM%252fQkmqKNoUidY7bXjPqOzaXZfqS9qrp55bj79ludUr4VLcG1FqHXzI%252fnEZb6Gg8pzytrnrgQFlDD4qhZPoL773oMaOt5Xu7Zj6UYRpAMqFbr0QakvMVWMSvw93s%253d; lang=v=2&lang=en-us; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; li_mc=MTsyMTsxNzEyMjI1NDc1OzI7MDIx4g1UjJLNxrgq1LN73193ANa7nJGrX7QYtI3diLCYb2I=; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; liap=true; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4190:u=253:x=1:i=1712225077:t=1712308566:v=2:sig=AQFhNZWPI_v5oL4y8oX9xLFL8a3cbxtv"; lms_ads=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lms_analytics=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; test=cookie; visit=v=1&M; JSESSIONID="ajax:2715582253737539260"; g_state={"i_l":0}; li_alerts=e30=; li_at=AQEDASvMh7YD9s79AAABjqiLGqYAAAGOzJeepk0AeYx2DWyrkdJ2zOVnqqljd2pif0w70vXt5CAmfT-Fzviq450QuPbnNpN17uHRhNTjn38eeZfAzJg70FJChZAL8U0ElXl--_qooC9a45fdzqkaU7Sv; li_g_recent_logout=v=1&true; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; li_theme=light; li_theme_set=app; timezone=Europe/Stockholm',
#         'csrf-token': 'ajax:2715582253737539260',
#         'sec-fetch-mode': 'cors'
#     }

#     total = None
#     for attempt in range(max_retries):
#         try:
#             response = requests.request("GET", api_request_url, headers=headers, data=payload)
#             if response.status_code == 200:
#                 data = response.json()
#                 paging = data.get('data', {}).get('paging', {})
#                 if paging:
#                     total = paging.get('total')
                
#                 if isinstance(total, int):
#                     return total
#             else:
#                 time.sleep(random.randint(3,5))
#                 continue

#         except requests.exceptions.RequestException as e:
#             print(f"Request failed: {e}")
#             time.sleep(random.randint(3,5))
#     return None     

def get_total_number_of_results_with_filters_applied(keyword, filters, max_retries=2):
    exp_level = filters.get('Experience Level', None)
    job_function = filters.get('Job Function', None)
    remote_options = filters.get('Remote Options', None)
    api_request_url = f"https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-71&count=7&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_JOB_FILTER,keywords:{keyword},locationUnion:(geoId:105117694),selectedFilters:(sortBy:List(R),experience:List({exp_level}),function:List({job_function}),workplaceType:List({remote_options})),spellCorrectionEnabled:true)"
    payload = {}
    headers = {
    'cookie': 'bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"; li_alerts=e30=; g_state={"i_l":0}; timezone=Europe/Stockholm; li_theme=light; li_theme_set=app; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; _gcl_au=1.1.308589430.1710419664; aam_uuid=16424388958969701103162659259461292262; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; visit=v=1&M; liap=true; JSESSIONID="ajax:2715582253737539260"; li_at=AQEDASvMh7YEt9E5AAABjshqKboAAAGO7Hatuk0ACEyiT8Cc7Mhr7II09PUxDOXuB80PJxTkP15y3JC6kqUVjKSigoozjgEdtJ5GwSNextmsTZm2xVaUbt-fJpNBVf6a536hj3bw8Fa9Xjiew7R0q7om; AnalyticsSyncHistory=AQJu2sHBLj8srgAAAY7WAMTX29daQ3Qr1pi5o3_tCFo5u2faZh4-RpCZSzOdgd891Za9N31IILgnazJBdjWG1g; lms_ads=AQHu20dgf3AFBwAAAY7WAMWp2_CAKaE4r6_hBb10yV3Hbhk6lPIoPE8ktiU_WenNapwMKQAkXwrjC4yB2Ijs8tDnFyWoW8E_; lms_analytics=AQHu20dgf3AFBwAAAY7WAMWp2_CAKaE4r6_hBb10yV3Hbhk6lPIoPE8ktiU_WenNapwMKQAkXwrjC4yB2Ijs8tDnFyWoW8E_; lang=v=2&lang=en-us; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvq6LTSmwlfNkKYZfUiiiuSqzMKS%252bEW2OpUyqEXmHObLf8Y2OW7uopb%252fyDKyGWyalTL2%252bAUxL46e47VH2o%252fV6HCW0tV25o9wI7Cv5T9xhjoLXe7122yYHwOKWghV593ZyiiYjmbvdfw5ouqIikdvT6qe9B8juVuSqI8QKi1EwolCDXQ8%252fCAyqmwQYt3EeGtRNMyQ3%252fG3oIT7D3vttALqi6Ouk%253d; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19829%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1713783556%7C6%7CMCAAMB-1713783556%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1713185956s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; UserMatchHistory=AQJOrE_wNLO1_QAAAY7hl2NCmJZv-VPWtL7uE1u-vvdtCFyyBTeXAuTFAeeA1HLl9n04H3azOPd5ci5z8JDucQTzfMBsFp6fHZgCh1PwSZLxnUqLhi7ypkhALB_wK2iYFBrFwMjuL88SL3QcUCxIVeBusmPeFXuq2EJTPv5HG0CmdCEaMX5J6rEUkBcaxcsdUB1gqhAgChDvKb7VMtBJBhf45dEuQ4V9NiDzPl4v8ZpfmfXHdrTQcAlZgDo0bNtV5Nlxih0OwCT6ru9JPW0TTa8ggPtzZWTXOXfQ3Co11AtZYLLEvngMeb9ytis1XeIK3cxRceU; sdsc=22%3A1%2C1713181779277%7EJAPP%2C0hz1ErKh5dvVymz8djGAhrry8Cws%3D; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4224:u=259:x=1:i=1713181779:t=1713255454:v=2:sig=AQEucnbsB22Am9kJXE6jZWzke0Ay-Bz2"; __cf_bm=VExjUvWnSAdMvNaQQBgyGrLlWPNuvNJM6pJYsZ_.Sjo-1713181798-1.0.1.1-nWHDPrVfFE68rg_V7Hdt7Bur0JHW9n5kvuRgk4hSDOafO7XU._bJaZ9QnsS9jbJbD3vl.7eWvCBtlpaQYkFEZA; li_mc=MTsyMTsxNzEzMTgxOTU4OzI7MDIxnIuI4RvrQ4PMg6dI0N2DY5Tg4a1fyd9d38FVxID2HRE=; bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTIzMjQ1NDQ7MjswMjH7F5RtGOB+6km5SD86xeRCXtAv1pzxFaZgc9eOZ6nlGQ==; li_mc=MTsyMTsxNzEzMTgyNTg1OzI7MDIx4qTh2R+tBZZTtzBmNF6PbW5+ngnhpIXeAJnlR1/voEY=',
    'csrf-token': 'ajax:2715582253737539260',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin'
    }

    total = None
    for attempt in range(max_retries):
        try:
            response = requests.request("GET", api_request_url, headers=headers, data=payload)
            print(f"Getting total amount API request response status code: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                paging = data.get('paging', {})
                if paging:
                    total = paging.get('total')
                    print(f"Total in the API call function: {total}")
                
                if isinstance(total, int):
                    return total
            else:
                time.sleep(random.randint(3,5))
                continue

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(random.randint(3,5))
    return None   

# We can only fetch 100 at a time
def split_total_into_batches_of_100(total):
    batches = [(i, i + 100) for i in range(0, total, 100)]
    # Adjust the last batch to not exceed the total
    if batches:
        batches[-1] = (batches[-1][0], total)
    return batches

def fetch_job_posting_ids(keyword, filters, batch, max_retries=2):
    print(f"Keyword: {keyword}, Filters: {filters}, Batch: {batch}")
    exp_level = filters.get('Experience Level', None)
    job_function = filters.get('Job Function', None)
    remote_options = filters.get('Remote Options', None)
    print(f"exp_level: {exp_level}, job_function: {job_function}, remote_options: {remote_options}")
    print(type(job_function))
    start, stop = batch
    batch_size = stop - start

    api_request_url = f"https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-72&count={batch_size}&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_JOB_FILTER,keywords:{keyword},selectedFilters:(sortBy:List(R),experience:List({exp_level}),function:List({job_function}),workplaceType:List({remote_options})),spellCorrectionEnabled:true)&start={start}"
    print(f"URL. {api_request_url}")
    headers = {
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'cookie': 'bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"; li_alerts=e30=; g_state={"i_l":0}; timezone=Europe/Stockholm; li_theme=light; li_theme_set=app; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; _gcl_au=1.1.308589430.1710419664; aam_uuid=16424388958969701103162659259461292262; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; visit=v=1&M; liap=true; JSESSIONID="ajax:2715582253737539260"; AnalyticsSyncHistory=AQJpWfcUFXP5iQAAAY7x6VX1oGgE6MMktoHnw1qIF83xzhnDk6e5g17fw61l_RHBegBy-dsHJ6iqi60Wkaxv4A; lms_ads=AQHgSjyO00BYDgAAAY7x6Vht2JKhxk_Fu3pfw4pyAz-Znm31IJbyXh2c1RUZTd6suZqN9CRyoQ5o46_V-ccHe4PInDicwCRL; lms_analytics=AQHgSjyO00BYDgAAAY7x6Vht2JKhxk_Fu3pfw4pyAz-Znm31IJbyXh2c1RUZTd6suZqN9CRyoQ5o46_V-ccHe4PInDicwCRL; li_at=AQEDASvMh7YEt9E5AAABjshqKboAAAGPFfXcSE0AVj1Dh-FEjTwpM4Jw2zlZE3bjTjPZxoGRph2JWMtDYv0_DivXnt_TBHLkdX18zsleL5H2Mpq1scbviswRvIK85QYwTIU5FLrqVMh0LwZaCs_G6NbN; lang=v=2&lang=en-us; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvq%252bfljF71dS3IUT1ewOwwOzVEGVtWkImVPo223ST3rs4b%252fVU2xBrMmG48OSrzPVTv7RisXlkeiE31yp7TZaPaa6PSL8njFWOjKrFbJWmqNQ5%252bPbY3am6FWsV75A6PzaLJVeU2MMMwXBIqh%252f78lSIFXUphUVlSvUj63t3ypyDX33TYm97LI3fzz5MyaaPnrDMySLv9SQDKjBdaeqBwfJtLlvQ%253d; li_mc=MTsyMTsxNzEzNDk4NTI4OzI7MDIx1oDUb6QA9Jq4kgb+FHlORuDzp/tXu0JIZmQN4Ap2DNQ=; UserMatchHistory=AQIeFAjVTDhqfAAAAY70eKtNo5MAqCY4MmA23-0v0w2pO5k5u25LDzJmw1Cefhq7g1VkeVhElE0T3Si2aSd6Z4g_JbJ0bgZxJ1b8xi3qdZoqTukBtJCCrleyo03DGmTE-cE4BhhoKExIBsYwkJYmrpJIY95gZ-ybAe67HQ49JV7-SGHaZoychdVVvDApq80r5c3NXrMJ3bNStnrzaLZotXoLCWQXUzLdesBtS85-aj6zQVh0ZiWlUZI_r7_z8Ci-ZhW1AI7hwPjXivclXqL8HOhrdpyRCzb4R7AKDgpPQdPAiibvB_cc02Z_SiRuZlS1PfBg4KY; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4231:u=261:x=1:i=1713498534:t=1713541985:v=2:sig=AQFEBA8rVHXF2bMCyaFyApPu8G5Y4zMi"; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19832%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1714103337%7C6%7CMCAAMB-1714103337%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1713505737s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; sdsc=22%3A1%2C1713498989738%7EJAPP%2C0Kxss%2BepGCGI6vxRQvUUuu5kl4zg%3D; bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTIzMjQ1NDQ7MjswMjH7F5RtGOB+6km5SD86xeRCXtAv1pzxFaZgc9eOZ6nlGQ==; li_mc=MTsyMTsxNzEzNDk5OTM3OzI7MDIx4lVvBH7Y5+OHnMIFbd2DBeeZbBNZaOI0CrCacFmOmCc=; sdsc=22%3A1%2C1713499000620%7EJAPP%2C071SDzb2InNOnl823SIddTo1Zo8A%3D; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"',
    'csrf-token': 'ajax:2715582253737539260',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin'
    }

    job_posting_ids_list = []
    for attempt in range(max_retries):
        try:
            response = requests.request("GET", api_request_url, headers=headers)
            print(f"Response status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()

                # Navigate to jobCardPrefetchQueries and get the first item's prefetchJobPostingCardUrns
                prefetch_job_posting_card_urns = data.get('data', {}) \
                                                    .get('metadata', {}) \
                                                    .get('jobCardPrefetchQueries', [{}])[0] \
                                                    .get('prefetchJobPostingCardUrns', [])
                for urn in prefetch_job_posting_card_urns:
                    job_posting_id_search = re.search(r"urn:li:fsd_jobPostingCard:\((\d+),JOB_DETAILS\)", urn)
                    if job_posting_id_search:
                        job_posting_id = job_posting_id_search.group(1)
                        print(f"Job posting id: {job_posting_id}")
                        job_posting_ids_list.append(job_posting_id)
            else:
                time.sleep(random.randint(3,5))
                continue

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(random.randint(3,5))
    return job_posting_ids_list #Return the list, even if its empty

def extract_all_job_posting_ids(keyword, filters, batches):
    job_posting_ids = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_job_posting_ids, keyword, filters, batch) for batch in batches]
        for future in as_completed(futures):
            job_posting_ids.extend(future.result())
    return job_posting_ids

def extract_full_name_bio_and_linkedin_url(job_posting_id, max_retries=2):
    api_request_url = f"https://www.linkedin.com/voyager/api/graphql?variables=(cardSectionTypes:List(HIRING_TEAM_CARD),jobPostingUrn:urn%3Ali%3Afsd_jobPosting%3A{job_posting_id},includeSecondaryActionsV2:true)&queryId=voyagerJobsDashJobPostingDetailSections.0a2eefbfd33e3ff566b3fbe31312c8ed"
    headers = {
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'cookie': 'bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"; li_alerts=e30=; g_state={"i_l":0}; timezone=Europe/Stockholm; li_theme=light; li_theme_set=app; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; _gcl_au=1.1.308589430.1710419664; aam_uuid=16424388958969701103162659259461292262; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; visit=v=1&M; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AnalyticsSyncHistory=AQInqKM9VjeJfgAAAY6jDr95ykAKgdVEJ-lmi2hFEpuwpHs0GW_s9vj-G4Uw6j1j_pUJJhZMGdSj03dRsS-GKQ; lms_ads=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lms_analytics=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19817%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1712823944%7C6%7CMCAAMB-1712823944%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1712226344s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvqxsjRiMumDMdY2cZBC7rnBcwKqNM68r3TpZblRKHzhjqTvmVAWbcHGdsb5IwTFqJY%252fMUYh2Qg2S1xLvrOKsF819j5MizM%252fQkmqKNoUidY7bXjPqOzaXZfqS9qrp55bj79ludUr4VLcG1FqHXzI%252fnEZb6Gg8pzytrnrgQFlDD4qhZPoL773oMaOt5Xu7Zj6UYRpAMqFbr0QakvMVWMSvw93s%253d; li_g_recent_logout=v=1&true; lang=v=2&lang=en-us; li_at=AQEDASvMh7YD9s79AAABjqiLGqYAAAGOzJeepk0AeYx2DWyrkdJ2zOVnqqljd2pif0w70vXt5CAmfT-Fzviq450QuPbnNpN17uHRhNTjn38eeZfAzJg70FJChZAL8U0ElXl--_qooC9a45fdzqkaU7Sv; liap=true; JSESSIONID="ajax:2715582253737539260"; li_mc=MTsyMTsxNzEyMjI0NzMyOzI7MDIxzODtaxUnhH03NiFJxX2nAcg+Zt1SBwH5NvsRAxtAtmk=; UserMatchHistory=AQKpLFT71zoM5QAAAY6olBvzmoHGZBhPQlhG2QJfDL6VSRwwrqxGU5OYng_P7oC3i705LjK1mJLCoudXGg-J0NDW4inNM4LtM90f1IHjAKPkiKBQOsqx7x89ZsAUgQ_Id-tKl50XVNuPZnAfsIVhngEuxkV6538FxYjln7OKcc94E830eKTIGCzm9sUFevFdtaLpUziPshqg7A5qPAlpsBx_ltoEvRBdb6eZSTz-zYDAogKN9htKasaYbT-8BPcjbuJVhvAmT24k4rFh07c3Zx78yU0PaPxnN68ue8yS7BangTpKgFAr9JustG_rujNj9mHuB9A; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4190:u=253:x=1:i=1712225263:t=1712308566:v=2:sig=AQGNtWMcukq66YUQAxEHE2Y2woh1guTQ"; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19817%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1712823944%7C6%7CMCAAMB-1712823944%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1712226344s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; AnalyticsSyncHistory=AQInqKM9VjeJfgAAAY6jDr95ykAKgdVEJ-lmi2hFEpuwpHs0GW_s9vj-G4Uw6j1j_pUJJhZMGdSj03dRsS-GKQ; UserMatchHistory=AQKz6POJSiLt1AAAAY6okULilyFDXuLLHMAMYVzy-IMAs6Dlwno_fksOjnrsAnsZpD2MUiiNSG9oGzLrbQNa4N5CTJqA0FOGwUAH3-vZ9blAScHMZjWEElHwe_wJf4WbR02jFr8oZXirGt2T5fmAiHm_27xgkRrk0ivUr11nWHvdhh6l_QpEkhkJhkL1gItuDoH1ok95GHg4SC0rIoD7Txfw0C_QUZnpE8oMvyyScBkPIIwEBHuDwDKIW9Bd8LPkVpLt-FRLcxHxceXm1RjE12H6A3hq8Hmugcmg5htGzvIiW-lBiKnLsYGUSPowBkKdDtoFxVk; __cf_bm=4CjrjV9LRvgqYwoA8OS1IyjQ7zH3YVil1zG0JmeJpeo-1712228179-1.0.1.1-eLHLoYutjvXBo4GP8r1gedbr1ZJCFp3NxwBNOQmMKxL0vYZzyhMKm1vSPtJNItNyNJVzPeN0y_iwqb95HuDArA; _gcl_au=1.1.308589430.1710419664; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; aam_uuid=16424388958969701103162659259461292262; bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvqxsjRiMumDMdY2cZBC7rnBcwKqNM68r3TpZblRKHzhjqTvmVAWbcHGdsb5IwTFqJY%252fMUYh2Qg2S1xLvrOKsF819j5MizM%252fQkmqKNoUidY7bXjPqOzaXZfqS9qrp55bj79ludUr4VLcG1FqHXzI%252fnEZb6Gg8pzytrnrgQFlDD4qhZPoL773oMaOt5Xu7Zj6UYRpAMqFbr0QakvMVWMSvw93s%253d; lang=v=2&lang=en-us; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; li_mc=MTsyMTsxNzEyMjI4MjAwOzI7MDIxb94IJj2ruSLZxsh1Efnbk8prKJ9zkDCjFP/uVoEiGwc=; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; liap=true; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4190:u=253:x=1:i=1712225077:t=1712308566:v=2:sig=AQFhNZWPI_v5oL4y8oX9xLFL8a3cbxtv"; lms_ads=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lms_analytics=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; test=cookie; visit=v=1&M; JSESSIONID="ajax:2715582253737539260"; g_state={"i_l":0}; li_alerts=e30=; li_at=AQEDASvMh7YD9s79AAABjqiLGqYAAAGOzJeepk0AeYx2DWyrkdJ2zOVnqqljd2pif0w70vXt5CAmfT-Fzviq450QuPbnNpN17uHRhNTjn38eeZfAzJg70FJChZAL8U0ElXl--_qooC9a45fdzqkaU7Sv; li_g_recent_logout=v=1&true; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; li_theme=light; li_theme_set=app; timezone=Europe/Stockholm',
    'csrf-token': 'ajax:2715582253737539260',
    'sec-fetch-mode': 'cors'
    }

    full_name = bio = linkedin_url = None
    for attempt in range(max_retries):
        try:
            response = requests.request("GET", api_request_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                elements = data.get('data', {}) \
                .get('data', {}) \
                .get('jobsDashJobPostingDetailSectionsByCardSectionTypes', {}) \
                .get('elements', [])

                if elements:
                    jobPostingDetailSection = elements[0].get('jobPostingDetailSection', [])
                    if jobPostingDetailSection:
                        hiring_team_card = jobPostingDetailSection[0].get('hiringTeamCard', {})
                        if hiring_team_card:
                            full_name = hiring_team_card.get('title', {}).get('text', None)
                            bio = hiring_team_card.get('subtitle', {}).get('text', None)
                            linkedin_url = hiring_team_card.get('navigationUrl')
                            return (full_name, bio, linkedin_url)
                    
                    return (full_name, bio, linkedin_url)
            else:
                time.sleep(random.randint(3,5))
                continue

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(random.randint(3,5))
    return None, None, None  # Return None if all attempts fail

def split_and_clean_full_name(full_name):
    # Remove parentheses and content within them
    cleaned_name = re.sub(r'\(.*?\)', '', full_name)
    # Further clean the name by removing any non-word and non-space characters
    cleaned_name = re.sub(r'[^\w\s]', '', cleaned_name, flags=re.UNICODE)
    name_parts = cleaned_name.split()
    first_name = name_parts[0] # We assume the first part will always be the first name after removing emojis etc
    last_name = name_parts[-1] if len(name_parts) > 1 else ''  # Check to avoid index error if name_parts is empty
    return (first_name, last_name)

def extract_company_info(job_posting_id, max_retries=2):
    api_request_url = f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_posting_id}?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-65"
    headers = {
    'cookie': 'bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"; li_alerts=e30=; g_state={"i_l":0}; timezone=Europe/Stockholm; li_theme=light; li_theme_set=app; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; _gcl_au=1.1.308589430.1710419664; aam_uuid=16424388958969701103162659259461292262; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; visit=v=1&M; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AnalyticsSyncHistory=AQInqKM9VjeJfgAAAY6jDr95ykAKgdVEJ-lmi2hFEpuwpHs0GW_s9vj-G4Uw6j1j_pUJJhZMGdSj03dRsS-GKQ; lms_ads=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lms_analytics=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvqxsjRiMumDMdY2cZBC7rnBcwKqNM68r3TpZblRKHzhjqTvmVAWbcHGdsb5IwTFqJY%252fMUYh2Qg2S1xLvrOKsF819j5MizM%252fQkmqKNoUidY7bXjPqOzaXZfqS9qrp55bj79ludUr4VLcG1FqHXzI%252fnEZb6Gg8pzytrnrgQFlDD4qhZPoL773oMaOt5Xu7Zj6UYRpAMqFbr0QakvMVWMSvw93s%253d; lang=v=2&lang=en-us; li_at=AQEDASvMh7YD9s79AAABjqiLGqYAAAGOzJeepk0AeYx2DWyrkdJ2zOVnqqljd2pif0w70vXt5CAmfT-Fzviq450QuPbnNpN17uHRhNTjn38eeZfAzJg70FJChZAL8U0ElXl--_qooC9a45fdzqkaU7Sv; liap=true; JSESSIONID="ajax:2715582253737539260"; li_mc=MTsyMTsxNzEyMjMxMjI0OzI7MDIxEmEZDdiE8GxoqN6bX3J51PVMWKjq87vUcQ1i/o/0Ud4=; __cf_bm=K9OPT3Ix0HrD_IlTjDYANLCxbMHYJZSLqN7MszCxptk-1712231585-1.0.1.1-QVawzdfwQRsHtxtjtR7t68YJ9yfJeneTlD.32ll7xwvsdS1kKIo3SABPdttcUeIR0iPsRDtjdb_q7N0lslqcmQ; UserMatchHistory=AQK6RfznW9o3xgAAAY6o97vkHlUeIQxGEtFzhFDFtcdFCAXQ665jLZTUQDt5EhpMFQPjk2WmeQ7Qb0Db1YCn9tJiBpIEo24K6w78N264yKbaelqGwpS16pEbCoTE7zROlWUm2CITfdk6Ka-JMPL3lgfwFi-7PTXMmQZ4D-D3HeLdlP0_xeMBiY8tpy_H1dc8ygAfahxdFkzHoNjZzcGoG6K-iev7nQDQr2bneg1hPH83U2INoZLyhWxpLrasJDC9Ks3NS3bDogocBkI7-1qwGLs4i-MUfsc0aKJXkf6qgidM0r3IDfnl9wNXruOvgsPsXgRL3tw; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4190:u=253:x=1:i=1712231793:t=1712308566:v=2:sig=AQFzt72littBzVmdj0NAzrfjuaRlQspM"; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19817%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1712836595%7C6%7CMCAAMB-1712836595%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1712238995s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19817%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1712823944%7C6%7CMCAAMB-1712823944%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1712226344s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; AnalyticsSyncHistory=AQInqKM9VjeJfgAAAY6jDr95ykAKgdVEJ-lmi2hFEpuwpHs0GW_s9vj-G4Uw6j1j_pUJJhZMGdSj03dRsS-GKQ; UserMatchHistory=AQKz6POJSiLt1AAAAY6okULilyFDXuLLHMAMYVzy-IMAs6Dlwno_fksOjnrsAnsZpD2MUiiNSG9oGzLrbQNa4N5CTJqA0FOGwUAH3-vZ9blAScHMZjWEElHwe_wJf4WbR02jFr8oZXirGt2T5fmAiHm_27xgkRrk0ivUr11nWHvdhh6l_QpEkhkJhkL1gItuDoH1ok95GHg4SC0rIoD7Txfw0C_QUZnpE8oMvyyScBkPIIwEBHuDwDKIW9Bd8LPkVpLt-FRLcxHxceXm1RjE12H6A3hq8Hmugcmg5htGzvIiW-lBiKnLsYGUSPowBkKdDtoFxVk; _gcl_au=1.1.308589430.1710419664; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; aam_uuid=16424388958969701103162659259461292262; bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvqxsjRiMumDMdY2cZBC7rnBcwKqNM68r3TpZblRKHzhjqTvmVAWbcHGdsb5IwTFqJY%252fMUYh2Qg2S1xLvrOKsF819j5MizM%252fQkmqKNoUidY7bXjPqOzaXZfqS9qrp55bj79ludUr4VLcG1FqHXzI%252fnEZb6Gg8pzytrnrgQFlDD4qhZPoL773oMaOt5Xu7Zj6UYRpAMqFbr0QakvMVWMSvw93s%253d; lang=v=2&lang=en-us; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; li_mc=MTsyMTsxNzEyMjMyMjk5OzI7MDIxuC3zI1LQwxwweI0/TTbN/C4WmBh+e9F0RtAxD38DujI=; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; liap=true; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4190:u=253:x=1:i=1712225077:t=1712308566:v=2:sig=AQFhNZWPI_v5oL4y8oX9xLFL8a3cbxtv"; lms_ads=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lms_analytics=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; test=cookie; visit=v=1&M; JSESSIONID="ajax:2715582253737539260"; g_state={"i_l":0}; li_alerts=e30=; li_at=AQEDASvMh7YD9s79AAABjqiLGqYAAAGOzJeepk0AeYx2DWyrkdJ2zOVnqqljd2pif0w70vXt5CAmfT-Fzviq450QuPbnNpN17uHRhNTjn38eeZfAzJg70FJChZAL8U0ElXl--_qooC9a45fdzqkaU7Sv; li_g_recent_logout=v=1&true; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; li_theme=light; li_theme_set=app; timezone=Europe/Stockholm',
    'csrf-token': 'ajax:2715582253737539260',
    'sec-fetch-mode': 'cors'
    }

    job_title = company_name = staff_count = staff_range = company_url = company_industry = companyID = None
    for attempt in range(max_retries):
        try:
            response = requests.request("GET", api_request_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                job_title = data.get('title', None)

                companyResolutionResult = data.get('companyDetails', {}) \
                    .get('com.linkedin.voyager.deco.jobs.web.shared.WebJobPostingCompany', {}) \
                    .get('companyResolutionResult', {})
                company_name = companyResolutionResult.get('name', None)
                staff_count = companyResolutionResult.get('staffCount', None)

                staff_range_lower = companyResolutionResult.get('staffCountRange', {}).get('start', None)
                staff_range_upper = companyResolutionResult.get('staffCountRange', {}).get('end', None)
                if staff_range_lower is not None and staff_range_upper is not None:
                    staff_range = f"{staff_range_lower} - {staff_range_upper}"
                elif staff_range_lower is not None:
                    staff_range = f"{staff_range_lower}+"
                elif staff_range_upper is not None:
                    staff_range = f"<{staff_range_upper}"
                else:
                    staff_range = "Okänt"

                companyID_search = re.search(r'(\d+)$', companyResolutionResult.get('entityUrn', ''))
                companyID = companyID_search.group(1) if companyID_search else None
                company_url = companyResolutionResult.get('url', None)
                industries = companyResolutionResult.get('industries', [])
                company_industry = industries[0] if industries else None

                return (job_title, company_name, staff_count, staff_range, company_url, company_industry, companyID)
            else:
                time.sleep(random.randint(3,5))
                continue

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(random.randint(3,5))
    return None, None, None, None, None, None, None

def extract_non_hiring_person(company_id, keywords, max_people_per_company, max_retries=2): 
    keywords_list = keywords.lower().split(", ")
    url_formatted_keywords = keywords.strip().replace(', ', '%20OR%20').replace(' ', '%20')
    
    api_request_url = f"https://www.linkedin.com/voyager/api/graphql?variables=(start:0,origin:FACETED_SEARCH,query:(keywords:{url_formatted_keywords},flagshipSearchIntent:ORGANIZATIONS_PEOPLE_ALUMNI,queryParameters:List((key:currentCompany,value:List({company_id})),(key:resultType,value:List(ORGANIZATION_ALUMNI))),includeFiltersInResponse:true),count:12)&queryId=voyagerSearchDashClusters.aacf309cb55f24005e058d2cf30a95ad"
    headers = {
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'cookie': 'bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_gc=MTswOzE3MTA0MTk0MzU7MjswMjE2GFD4tGaA955A7K5M9w3OxKao0REV7R8R3/LDZ/ZVJQ==; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"; li_alerts=e30=; g_state={"i_l":0}; timezone=Europe/Stockholm; li_theme=light; li_theme_set=app; _guid=9d344ac1-8a69-44f0-ba51-4e8884d4ccac; li_sugr=6fadc81f-40bf-4c11-9bc8-f36f95783541; _gcl_au=1.1.308589430.1710419664; aam_uuid=16424388958969701103162659259461292262; dfpfpt=2585905f65d4454db4b2923a3ee8bc24; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; visit=v=1&M; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AnalyticsSyncHistory=AQInqKM9VjeJfgAAAY6jDr95ykAKgdVEJ-lmi2hFEpuwpHs0GW_s9vj-G4Uw6j1j_pUJJhZMGdSj03dRsS-GKQ; lms_ads=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lms_analytics=AQEPbpVkVUBMJwAAAY6jDsDdSL3Mw1m_OduZrR3hlmqPxRHRs1Ajcc5Zo_Z8pOj-Kl3vtbYD-sa69Co_lrctHDJKkWtAjACm; lang=v=2&lang=en-us; li_at=AQEDASvMh7YD9s79AAABjqiLGqYAAAGOzJeepk0AeYx2DWyrkdJ2zOVnqqljd2pif0w70vXt5CAmfT-Fzviq450QuPbnNpN17uHRhNTjn38eeZfAzJg70FJChZAL8U0ElXl--_qooC9a45fdzqkaU7Sv; liap=true; JSESSIONID="ajax:2715582253737539260"; fptctx2=taBcrIH61PuCVH7eNCyH0MJojnuUODHcZ6x9WoxhgCkAr9en60wAbfeXvyW5bYQhcX76e9lzuPfcckEKYDk1omjn%252fBbajvM3A%252f0ra5KWWbn6CpB5ts0e8OrCs%252bDiqyP2v4aXF1Cod4M2QlHSbNcvqyIS3EHNCaQb1yqedO0SnYQMhwbJN8ySTkCer5wo%252boj3R6Kjve%252b4U7H4WVzqtUgXP7YM6GRQ85K3mDGXrbXg4ZNAFi8w%252bd5zuD%252bFIZ37zTZrbmHBabFntMHiqadB2DZiIaiWEgi%252fl1%252f%252fl1OFnBVL7ugO%252bj6bBBXxf2utkvg%252f%252fWY17NNVTXALlWyLCFcF9%252f%252fzFWz1bRB4FQl%252bLi2QIrLZ704%253d; li_mc=MTsyMTsxNzEyMzI2NTUxOzI7MDIxftb6aAVYVH8gDfsXaIlY3yeEj2uLVVHmOTq0a9yzQuA=; __cf_bm=MMMJZ77oY71_nUapoBIXf53UsEJngGvFl8HAcWNdXdo-1712326552-1.0.1.1-c3FwEI2LnDeCVPYN5aHWT.6roD15mx72mX_UDcXebBQKEiof3DkT_mxqF9tTZd2Gf53MFfnzBo6qYs6TzJhimg; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19819%7CMCMID%7C15864482448327108373110627159475528493%7CMCAAMLH-1712931354%7C6%7CMCAAMB-1712931354%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1712333754s%7CNONE%7CMCCIDH%7C-1259936587%7CvVersion%7C5.1.1; UserMatchHistory=AQIdxcAxC5cMuAAAAY6unqyHEzSFed0QmxyjHcYParVnDqvlBeBYOGzNn0wTj7EPMPwsdad0g63r8sUggTv0RYtoHjEbBQM1BddULbdJZMqB0jgeATGwUSvpHDz2HbNnUeMXt_-AG34fekU0NKkR-QB6GSIWrxgp7OwxMNNmOTdFO1LAN8eGCJjSsVdlZld1kxYvPkgRrwgldSmrmxaNIYf4HmzDvSNAn81fFZjjbM282dCx-Eq1yuTrlstYQ-lJLpXWL3m_JNJmVNrN7tZGUUNnX8rWhqK2c2XqZIJQfgzopJ59VhuaYrXvYm7YoyKLm5N4ljg; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4206:u=253:x=1:i=1712326620:t=1712391543:v=2:sig=AQGUjv2njj0nMPTaqNmrFV0FtUrlqPGL"; bcookie="v=2&70aaf49d-ed55-4b5c-8362-d0eba01b2555"; li_gc=MTswOzE3MTIzMjQ1NDQ7MjswMjH7F5RtGOB+6km5SD86xeRCXtAv1pzxFaZgc9eOZ6nlGQ==; li_mc=MTsyMTsxNzEyMzEwNjM4OzI7MDIxtt/UHTCJyTLYmxgG2aQ96q5IGk7uCVBxDqWoZ+zeaDs=; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4206:u=253:x=1:i=1712326742:t=1712391543:v=2:sig=AQEGOhcsViLtSujkKRWPUgMfRKtbxSEg"',
    'csrf-token': 'ajax:2715582253737539260',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }

    processed_staff = []
    for attempt in range(max_retries):
        try:
            time.sleep(random.randint(1,3))
            response = requests.request("GET", api_request_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                # print(f"Data: {data}")
                included = data.get('included', [{}])

                # Filter out any items in 'included' that don't represent a person based on a minimum number of keys
                filtered_people = [person for person in included if len(person) >= 5]
                # print(filtered_people[:300])
                   
                for person in filtered_people:
                    if  len(processed_staff) >= max_people_per_company:
                        break # Exit the loop once we have enough people

                    if not isinstance(person, dict):
                        continue

                    linkedin_url = person.get('navigationUrl')
                    if isinstance(linkedin_url, str):
                        linkedin_url_search = re.search(r'^(.*?)\?', linkedin_url)
                        linkedin_url_result = linkedin_url_search.group(1) if linkedin_url_search else linkedin_url # Use the original URL if no query parameters are found
                    else:
                        linkedin_url_result = None
                    full_name = person.get('title', {}).get('text', None)
                    bio = person.get('primarySubtitle', {}).get('text', None)
                    
                    is_present = any(title in bio.lower() for title in keywords_list)
                    if is_present:
                        processed_staff.append(("FALSE", full_name, bio, linkedin_url_result))

                return processed_staff
            else:
                time.sleep(random.randint(3,5))
                continue

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(random.randint(3,5))
    return []

def hiring_person_or_not(job_posting_id, staff_threshold, under_threshold_keywords, over_threshold_keywords, max_people_per_company):
    full_name = bio = linkedin_url = None
    full_name, bio, linkedin_url = extract_full_name_bio_and_linkedin_url(job_posting_id)
    if full_name and bio and linkedin_url:
        hiring_team = "TRUE"
        return [(hiring_team, full_name, bio, linkedin_url)]
    else:
        job_title, company_name, staff_count, staff_range, company_url, company_industry, companyID = extract_company_info(job_posting_id)

        if staff_count:
            company_keywords = under_threshold_keywords if staff_count <= staff_threshold else over_threshold_keywords    
            company_people = extract_non_hiring_person(companyID, company_keywords, max_people_per_company)
            return company_people
        else:
            return []

def main(keyword, filters, batches, staff_threshold, under_threshold_keywords, over_threshold_keywords, max_people_per_company, max_workers=5):
    all_job_posting_ids = extract_all_job_posting_ids(keyword, filters, batches)
    if len(all_job_posting_ids) == 0:
        return 0
    print(f"All job posting ids: {all_job_posting_ids}")
    print(f"Length of all job postings: {len(all_job_posting_ids)}")

    grouped_results = []
    # Using ThreadPoolExecutor to manage a pool of threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Dictionary to keep track of futures and job postings
        future_to_company = {
            executor.submit(extract_company_info, job_posting): job_posting for job_posting in all_job_posting_ids
        }
        future_to_staff = {
            executor.submit(hiring_person_or_not, job_posting, staff_threshold, under_threshold_keywords, 
                            over_threshold_keywords, max_people_per_company): job_posting for job_posting in all_job_posting_ids
        }

    results = {job_posting: [None, None] for job_posting in all_job_posting_ids}  # Pre-initialize results dict with placeholders
    print(f"Entered threading stage...")

    counter = 0
    #Iterating over completed tasks as they complete
    for future in as_completed(future_to_company | future_to_staff):
        print(f"Processed: {counter}")
        if future:
            job_posting = future_to_company.get(future) or future_to_staff.get(future)
            try:
                data = future.result()
                if future in future_to_company:
                    results[job_posting][0] = data
                else:
                    results[job_posting][1] = data
                counter += 1
            except Exception as e:
                print(f"Job posting {job_posting} generated an exception: {e}")
                counter += 1
        else:
            print(f"Job posting {job_posting} returned None")
            counter += 1
            continue

    # Organizing the results
    for job_posting, data_pair in results.items():
        # Check if staff data (data_pair[1]) is not empty
        if data_pair[1]:  # This checks if staff_data is not empty
            # print(f"Data pair: {data_pair}")
            grouped_results.append((job_posting, tuple(data_pair)))
    
    if len(grouped_results) == 0:
        return 0
    return grouped_results

def process_staff_and_company_data(keyword, filters, person, company_data, job_posting_id):
    hiring_team, full_name, bio, linkedin_url = person
    job_title, company_name, staff_count, staff_range, company_url, company_industry, company_id = company_data

    first_name = last_name = row = None
    print(f"Filters: {filters}")
    print(f"Type of filters: {type(filters)}")
    exp_level = filters.get('Experience Level', None)
    job_function = filters.get('Job Function', None)
    remote_options = filters.get('Remote Options', None)
    print(type(exp_level))
    print(type(job_function))
    print(type(remote_options))
    if full_name:
        first_name, last_name = split_and_clean_full_name(full_name)
    if not full_name and staff_count is not None:
        company_keywords = under_threshold_keywords if staff_count <= staff_threshold else over_threshold_keywords
        url_formatted_keywords = company_keywords.strip().replace(', ', '%20OR%20').replace(' ', '%20')
        construced_url = f"{company_url}/people/?keywords={url_formatted_keywords}"
        row = {'Hiring Team':hiring_team, 'Förnamn':construced_url, 'Efternamn':last_name, 'Bio':bio, 
            'LinkedIn URL':linkedin_url, 'Jobbtitel som sökes':job_title, 'Jobbannons-URL':f"https://www.linkedin.com/jobs/search/?currentJobId={job_posting_id}&geoId=105117694&keywords={keyword}&location=Sweden&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R", 
            'Företag':company_name, 'Antal anställda':staff_range, 'Företagsindustri':company_industry, 'Företags-URL':company_url}
    else:
        row = {'Hiring Team':hiring_team, 'Förnamn':first_name, 'Efternamn':last_name, 'Bio':bio, 
            'LinkedIn URL':linkedin_url, 'Jobbtitel som sökes':job_title, 'Jobbannons-URL':f"https://www.linkedin.com/jobs/search/?currentJobId={job_posting_id}&geoId=105117694&keywords={keyword}&location=Sweden&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R", 
            'Företag':company_name, 'Antal anställda':staff_range, 'Företagsindustri':company_industry, 'Företags-URL':company_url}
    return row

def process_result(result):
    rows = []

    job_posting_id = result[0]
    if isinstance(result[1][1], list): # staff data will always be a list
        company_data, staff_data = result[1]
    else:
        staff_data, company_data = result[1]
    print(f"Company data: {company_data}")
    print(f"staff data: {staff_data}")

    first_person = staff_data[0]
    row = process_staff_and_company_data(first_person, company_data, job_posting_id)
    rows.append(row)
    
    if len(staff_data) > 1:
        second_person = staff_data[1]
        row = process_staff_and_company_data(second_person, company_data, job_posting_id)
        rows.append(row)
    return rows

def transform_grouped_results_into_df_parallel(grouped_results):
    processed_lists = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Execute process_result for each result in parallel
        for processed_result in executor.map(process_result, grouped_results):
            # processed_result is a list of dictionaries (rows)
            processed_lists.extend(processed_result)  # Extend the master list with these rows
    
    # Now processed_lists contains all rows as dictionaries; convert it to a DataFrame
    return pd.DataFrame(processed_lists)

def generate_csv(dataframe, result_name):
    if result_name.endswith('.csv'):
        result_name = result_name
    else:
        result_name = result_name + '.csv'
    dataframe.to_csv(result_name, index=False)
    return result_name

def generate_excel(dataframe, result_name):
    if result_name.endswith('.xlsx'):
        result_name = result_name
    else:
        result_name = result_name + '.xlsx'
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output

def convert_seconds_to_minutes_and_seconds(seconds):
    min, sec = divmod(seconds, 60)
    return '%02d:%02d' % (min, sec)

## STREAMLIT CODE
st.title('LinkedIn Job search URL to CSV Generator V2')
# st.markdown('Jobbar på att få det att funka 100% 🛠️ Just nu är det hårdkodat att den kollar efter max 2 personer per företag')
st.markdown(f'Sample URL: https://www.linkedin.com/jobs/search/?currentJobId=3842223191&f_F=mrkt&geoId=105117694&keywords=sem%20seo&location=Sweden&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R')

# User input for LinkedIn URL
linkedin_job_url = st.text_input('Skriv in en URL från LinkedIn Jobs:', '')
result_name = st.text_input('Namn på den resulterande csv/Excel filen:', '')
max_results_to_check = st.text_input('Max antal jobb att ta info från (lämna blankt för att ta alla tillgängliga i sökningen):', '')
st.write("Om det inte finns en Hiring Team och företaget har mindre än eller lika med")
staff_threshold = st.number_input("staff Threshold", min_value=1, value=50, step=1, format="%d", label_visibility="collapsed")
under_threshold_keywords = st.text_input('anställda, sök företaget efter (separera keywords med kommatecken):', '')
over_threshold_keywords = st.text_input('Om det har mer, sök företaget efter: (separera keywords med kommatecken)', '')
# max_people_per_company = st.number_input('Max antal anställda per företag som ska med i resultatet om det inte finns Hiring Team:', min_value=1, value=2, step=1, format="%d")
max_people_per_company = 2

# Radio button to choose the file format
file_format = st.radio("Välj filformat:", ('csv', 'xlsx'))

# Button to the result file
if st.button('Generera fil'):
    with st.spinner('Genererar fil, ett ögonblick'):
        if linkedin_job_url:
            keyword_search = re.search(r'keywords=([^&]+)', linkedin_job_url)
            linkedin_keyword = keyword_search.group(1) if keyword_search else None
            print(f"\nNew search! Keyword: {linkedin_keyword}")

            linkedin_filters = extract_filters_from_url(linkedin_job_url)
            print(f"Filters: {linkedin_filters}")
            print(f"Type of filters: {type(linkedin_filters)}")

            start_time = time.time()
            # total_number_of_results = get_total_number_of_results(keyword)
            total_number_of_results = get_total_number_of_results_with_filters_applied(linkedin_keyword, linkedin_filters)

            if max_results_to_check.strip():  # Checks if input is not just whitespace
                try:
                    max_results = int(max_results_to_check)
                    if max_results < total_number_of_results:
                        total_number_of_results = max_results
                except ValueError:
                    st.error("Please enter a valid number or leave blank for all jobs.")
            else:
                # Explicit handling for "all" case, if needed
                pass

            print(f"Total after transformation: {total_number_of_results}")
            print(type(total_number_of_results))
            print(f"Attempting to scrape info from {total_number_of_results} job ads")
            st.markdown(f"Tar info från {total_number_of_results} jobbannonser. Det tar ca 2.5 seconds per jobbannons, så detta kommer ta omkring {convert_seconds_to_minutes_and_seconds(total_number_of_results*2.5)} minuter men potentiellt snabbare!")

            batches = split_total_into_batches_of_100(total_number_of_results)
            print(f"Splitting {total_number_of_results} in batches: {batches}")

            results = main(linkedin_keyword, linkedin_filters, batches, staff_threshold, under_threshold_keywords, over_threshold_keywords, int(max_people_per_company))
            end_time = time.time()
            if results == 0:
                st.error("API status error. Try again later")
            else:
                print(f"Done! Length of results: {len(results)}")
                st.text(f"Done! Tog info från {total_number_of_results} annonser på {convert_seconds_to_minutes_and_seconds(end_time - start_time)} minuter")
                scraped_data_df = transform_grouped_results_into_df_parallel(results)

                if file_format == 'csv':
                    csv_file = generate_csv(scraped_data_df, result_name)
                    with open(csv_file, "rb") as file:
                        st.download_button(label="Ladda ner CSV", data=file, file_name=csv_file, mime='text/csv')
                    st.success(f'CSV-fil genererad: {csv_file}')
                elif file_format == 'xlsx':
                    excel_file = generate_excel(scraped_data_df, result_name)
                    st.download_button(label="Ladda ner xlsx", data=excel_file, file_name=f"{result_name}.xlsx", mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    st.success(f'Excel-file genererar: {result_name}.xlsx')
        else:
            st.error('Please enter a valid LinkedIn URL.')

# def turn_grouped_results_into_df(grouped_results):
#     results = {'Hiring Team':[], 'Förnamn':[], 'Efternamn':[], 'Bio':[], 'LinkedIn URL':[], 'Jobbtitel som sökes':[], 'Jobbannons-URL':[], 'Företag':[], 'Antal anställda':[], 'Anställda interall':[], 'Företagsindustri':[], 'Företags-URL':[]}

#     for result in grouped_results:
#         job_posting_id = result[0]
#         if isinstance(result[1][1], list): # staff data will always be a list
#             company_data, staff_data = result[1]
#         else:
#             staff_data, company_data = result[1]

#         print(f"Company data: {company_data}")
#         print(f"staff data: {staff_data}")

#         job_title, company_name, staff_count, staff_range, company_url, company_industry, company_id = company_data
#         print(f"Staff count when entering the DataFrame: {staff_count}")

#         for person in staff_data:
#             hiring_team, full_name, bio, linkedin_url = person
        
#             results['Hiring Team'].append(hiring_team)
#             if full_name:
#                 first_name, last_name = split_and_clean_full_name(full_name)
#                 results['Förnamn'].append(first_name)
#                 results['Efternamn'].append(last_name)
#             else:
#                 results['Efternamn'].append(None)
#                 if staff_count:
#                     company_keywords = under_threshold_keywords if staff_count <= staff_threshold else over_threshold_keywords
#                     url_formatted_keywords = company_keywords.strip().replace(', ', '%20OR%20').replace(' ', '%20')
#                     construced_url = f"{company_url}/people/?keywords={url_formatted_keywords}"
#                     results['Förnamn'].append(construced_url)
#                 else:
#                     results['Förnamn'].append(None)
            
#             results['Bio'].append(bio)
#             results['LinkedIn URL'].append(linkedin_url)
#             results['Jobbtitel som sökes'].append(job_title)
#             results['Jobbannons-URL'].append(f"https://www.linkedin.com/jobs/search/?currentJobId={job_posting_id}&geoId=105117694&keywords={keyword}&location=Sweden")
#             results['Företag'].append(company_name)
#             results['Antal anställda'].append(staff_count)
#             results['Anställda interall'].append(staff_range)
#             results['Företagsindustri'].append(company_industry)
#             results['Företags-URL'].append(company_url)

#     linkedin_jobs_df = pd.DataFrame.from_dict(results)
#     return linkedin_jobs_df

# job_function_filter = {'Advertising':'advr', 'Analyst':'anls', 'Art/Creative':'art', 
#                         'Consulting':'cnsl', 'Design':'dsgn', 'Engineering':'eng', 'Information Technology':'it', 
#                         'Management':'mgmt', 'Marketing':'mrkt', 'Sales':'sale', 
#                         'Project Management':'prjm'}
# experience_level_filter = {'Internship':1, 'Entry level':2, 'Associate':3, 'Mid-Senior level':4, 
#                            'Director':5, 'Executive':6}
# remote_filter = {'On-site':1, 'Remote':2, 'Hybrid':3}

# def create_checkboxes(filter_dict, title):
#     selected_values = []
#     st.write(title)
#     # Adjust the number of columns based on the title
#     if title == "Job Function":
#         num_cols = 5
#     elif title == "Experience Level":
#         num_cols = 3
#     else:
#         num_cols = len(filter_dict)
#     cols = st.columns(num_cols)

#     # Use itertools.cycle to cycle through the columns
#     cols_cycle = cycle(cols)
    
#     for key, value in filter_dict.items():
#         col = next(cols_cycle)  # Get the next column from the cycle
#         if col.checkbox(key):  # Create a checkbox in the column
#             selected_values.append(value)
    
#     return selected_values

# # Call the function for each dictionary and collect selected items
# selected_job_functions = create_checkboxes(job_function_filter, "Job Function")
# selected_experience_levels = create_checkboxes(experience_level_filter, "Experience Level")
# selected_remote_options = create_checkboxes(remote_filter, "Remote Options")

# filters = {"Job Function": selected_job_functions, "Exp level": selected_experience_levels, "Remote": selected_remote_options}

# # Example usage of the selected items
# if st.button("Print Selected Values"):
#     st.write("Selected Job Functions:", selected_job_functions)
#     st.write("Selected Experience Levels:", selected_experience_levels)
#     st.write("Selected Remote Options:", selected_remote_options)