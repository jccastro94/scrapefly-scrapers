import asyncio
import nest_asyncio
from collections import defaultdict
import json
from pathlib import Path
import re
import os
from typing import List, Optional
from urllib.parse import urlencode
from scrapfly import ScrapflyClient, ScrapeConfig, ScrapeApiResponse 
from parsel import Selector
import tracemalloc
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv

load_dotenv()

scrapfly = ScrapflyClient(key=os.getenv('SCRAPFLY_API_KEY'))


def save_progress(hotel_id, duration, last_date):
    progress = {
        "hotel_id": hotel_id,
        "duration": duration,
        "last_date": last_date
    }
    with open("booking.json", "w") as f:
        json.dump(progress, f)


async def scrape_prices(csrf_token, hotel_id, hotel_url, start_date, duration, days_to_check, resume_from=None):
    prices = {}
    start = datetime.strptime(start_date, "%Y-%m-%d")
    
    for day in range(days_to_check):
        check_date = start + timedelta(days=day)
        
        # If resuming, skip dates before the resume point
        if resume_from and check_date < datetime.strptime(resume_from, "%Y-%m-%d"):
            continue

        data = {
            "name": "hotel.availability_calendar",
            "result_format": "price_histogram",
            "hotel_id": hotel_id,
            "search_config": json.dumps(
                {
                    "b_adults_total": 2,
                    "b_nr_rooms_needed": 1,
                    "b_children_total": 0,
                    "b_children_ages_total": [],
                    "b_is_group_search": 0,
                    "b_pets_total": 0,
                    "b_rooms": [{"b_adults": 2, "b_room_order": 1}],
                }
            ),
            "checkin": check_date.strftime("%Y-%m-%d"),
            "n_days": duration,
            "respect_min_los_restriction": 1,
            "los": duration,
        }
        try:
            result = await scrapfly.async_scrape(
                ScrapeConfig(
                    url="https://www.booking.com/fragment.json?cur_currency=usd",
                    method="POST",
                    data=data,
                    headers={"X-Booking-CSRF": csrf_token},
                    session=hotel_url.split("/")[-1].split(".")[0],
                    country="US",
                )
            )
            price_data = json.loads(result.content)["data"]
            prices[check_date.strftime("%Y-%m-%d")] = price_data
        except Exception as e:
            print(f"Error fetching price for {check_date}: {str(e)}")
            prices[check_date.strftime("%Y-%m-%d")] = None

            save_progress(hotel_id, duration, check_date.strftime("%Y-%m-%d"))
            raise 
        
        await asyncio.sleep(1)
    
    return prices


def parse_hotel(html: str):
    sel = Selector(text=html)
    css = lambda selector, sep="": sep.join(sel.css(selector).getall()).strip()
    css_first = lambda selector: sel.css(selector).get("")
    lat, lng = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
    features = defaultdict(list)
    for feat_box in sel.css("[data-capla-component*=FacilitiesBlock]>div>div>div"):
        type_ = feat_box.xpath('.//span[contains(@data-testid, "facility-group-icon")]/../text()').get()
        feats = [f.strip() for f in feat_box.css("li ::text").getall() if f.strip()]
        features[type_] = feats
    data = {
        "title": css("h2#hp_hotel_name::text"),
        "description": css("div#property_description_content ::text", "\n"),
        "address": css(".hp_address_subtitle::text"),
        "lat": lat,
        "lng": lng,
        "features": dict(features),
        "id": re.findall(r"b_hotel_id:\s*'(.+?)'", html)[0],
    }
    return data


async def scrape_hotel_availability(url: str, start_date: str, durations: List[int], days_to_check: int, resume_from: Optional[str] = None):
    result = await scrapfly.async_scrape(ScrapeConfig(
        url, 
        session=url.split("/")[-1].split(".")[0],
        country="US",
    ))
    hotel = parse_hotel(result.content)
    hotel["url"] = result.context['url']
    csrf_token = re.findall(r"b_csrf_token:\s*'(.+?)'", result.content)[0]
    
    hotel["availability"] = {}
    for duration in durations:
        hotel["availability"][duration] = await scrape_prices(
            csrf_token=csrf_token,
            hotel_id=hotel["id"],
            hotel_url=url,
            start_date=start_date,
            duration=duration,
            days_to_check=days_to_check,
            resume_from=resume_from
        )
    
    return hotel


def load_progress():
    if os.path.exists("booking.json"):
        with open("booking.json", "r") as f:
            return json.load(f)
    return None


async def run_booking_scraper(hotel_urls: List[str], start_date: str, durations: List[int], days_to_check: int):
    out = Path(os.getcwd()) / "results"
    out.mkdir(exist_ok=True)
    
    results_file = out / "hotel_availability.json"
    
    # Load existing results if any
    if results_file.exists():
        with open(results_file, "r") as f:
            results = json.load(f)
    else:
        results = []

    processed_urls = set(hotel['url'] for hotel in results)

    for url in hotel_urls:
        if url in processed_urls:
            print(f"Skipping already processed URL: {url}")
            continue

        progress = load_progress()
        resume_from = progress["last_date"] if progress else None
        
        try:
            hotel_data = await scrape_hotel_availability(url, start_date, durations, days_to_check, resume_from)
            results.append(hotel_data)
            
            # Save the updated results after each hotel
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            print(f"Saved data for hotel: {url}")
        except Exception as e:
            print(f"Error scraping hotel {url}: {str(e)}")
            print(f"Progress saved. You can resume from the last successful date.")
            break
        
        await asyncio.sleep(5)

    print("Scraping completed.")