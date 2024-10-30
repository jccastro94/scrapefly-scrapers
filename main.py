from booking_scraper import run_booking_scraper
import asyncio

hotel_urls = [
    "https://www.booking.com/hotel/mx/grand-bahia-principe-coba-akumal8.es.html",
    "https://www.booking.com/hotel/mx/riu-lupita-all-inclusive.es.html",
    "https://www.booking.com/hotel/mx/sandos-caracol-beach-resort-and-spa.es.html",
    "https://www.booking.com/hotel/mx/sandos-playacar-beach-resort-spa.es.html",
    "https://www.booking.com/hotel/mx/viva-wyndham-maya.es.html",
    "https://www.booking.com/hotel/mx/iberostar-tucan.es.html"
]

start_date = "2024-08-08"
durations = [1, 3, 7]
days_to_check = 90

async def main():
    await run_booking_scraper(hotel_urls, start_date, durations, days_to_check)


if __name__ == "__main__":
    asyncio.run(main())