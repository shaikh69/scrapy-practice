import scrapy
from scrapy.cmdline import execute
import json
from oppvenuz.utils.utils import slugify
from datetime import datetime


class ScrapeOppvenuzSpider(scrapy.Spider):
    name = "scrape-oppvenuz"
    allowed_domains = ["oppvenuz.com"]
    start_urls = ["https://www.oppvenuz.com"]
    custom_settings = {
        "FEEDS": {
            "output/%(name)s_%(time)s.csv": {"format": "csv", "overwrite": False}
        },
    }

    def parse(self, response):
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7',
            'origin': 'https://www.oppvenuz.com',
            'referer': 'https://www.oppvenuz.com/',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36'
        }
        url = 'https://api.oppvenuz.com/api/feedbacks/v1/newserviceCityRelation'

        yield scrapy.Request(
            url=url,
            method='GET',
            callback=self.get_services,
            headers=headers,
            meta={'headers': headers}
        )

    def get_services(self, response):
        if response.status == 200:
            services = json.loads(response.text).get('data', {}).keys()

            if services:
                for service in services:
                    slug = slugify(service)
                    url = f'https://api.oppvenuz.com/api/service/v1/serviceSearchFilter?service_type={slug}&search=&city=&page=1&price_min=&price_max=&subscription=&title=&user_group='
                    headers = response.meta.get('headers')
                    yield scrapy.Request(
                        url=url,
                        method='GET',
                        callback=self.get_category,
                        headers=headers,
                        meta={'headers': headers, 'slug': slug, 'page': 1}  # Track page and slug for pagination
                    )
                    # break  # Run for only one service
        else:
            print(f"Failed to fetch services. Status: {response.status}")

    def get_category(self, response):
        data_dict = json.loads(response.text)

        if 'data' not in data_dict:
            print("No data found in response.")
            return

        pagination = data_dict['data'].get('links', {})
        next_page_url = pagination.get('next')
        headers = response.meta.get('headers')
        results = data_dict['data'].get('results', [])

        # Process results if they exist
        for result in results:
            id = result.get("id")
            url = f'https://api.oppvenuz.com/api/service/getVendorServiceDetails/{id}'
            yield scrapy.Request(
                url=url,
                method='POST',
                headers= headers,
                callback= self.get_service_detail,
            )

        # Check if there's a next page and make recursive request if it exists
        if next_page_url:
            yield scrapy.Request(
                url=next_page_url,
                method='GET',
                callback=self.get_category,
                headers=headers,
                meta=response.meta  # Preserve headers and other meta info
            )
            print("Fetching next page")
        else:
            print("No more pages to fetch.")

    def get_service_detail(self, response):
        json_data = json.loads(response.text)
        data = json_data['data'][0]
        item = {}
        best_suitable = []

        item["service_detail_id"] = data.get('id')
        item['vendor_id'] = data.get('vendor_id')
        item['vendor_name'] = data.get('vendor_name')
        item['vendor_contact'] = data.get('vendor_contact')
        item['service_id'] = data.get('service_id')
        item['service_type'] = data.get('service_type')
        item['service_type_code'] = data.get('service_type_code')
        item['business_name'] = data.get('business_name')
        item['business_image'] = data.get('business_image')
        item['working_since'] = data.get('working_since')
        item['number_of_events_done'] = data.get('number_of_events_done')
        item['user_group_service_type'] = data.get('user_group_service_type')
        item['website_url'] = data.get('website_url')
        item['facebook_url'] = data.get('facebook_url')
        item['instagram_url'] = data.get('instagram_url')
        item['area'] = data.get('area')
        item['city'] = data.get('city')
        item['state'] = data.get('state')
        item['pin_code'] = data.get('pin_code')
        item['service_attachments'] = data.get('service_attachments')
        item['service_pricing'] = data.get('service_pricing')
        item['share_url'] = data.get('share_url')

        created_at = data.get('created_at')
        if created_at:
            try:
                item['created_at'] = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S').isoformat()
            except ValueError:
                item['created_at'] = created_at

        updated_at = data.get('updated_at')
        if updated_at:
            try:
                item['updated_at'] = datetime.strptime(updated_at, '%Y-%m-%dT%H:%M:%S').isoformat()
            except ValueError:
                item['updated_at'] = updated_at

        best_suitable_for = data.get('best_suitable_for_detail')
        if best_suitable_for:
            for best in best_suitable_for:
                best_suitable.append(best.get('title'))
            item['best_suitable_for'] = best_suitable

        item['service_views'] = data.get('service_views')
        item['service_likes'] = data.get('service_likes')
        item['plan_data'] = data.get('plan_data')
        item['about_us'] = data.get('about_us')
        item['share_count'] = data.get('share_count')

        contact_details = data.get('contact_details')[0]
        if contact_details:
            item['contact_details_email'] = contact_details.get('contact_email')

        payment_cancellation_policy = data.get('payment_cancellation_policy')[0]
        if payment_cancellation_policy:
            item['advance_for_booking'] = payment_cancellation_policy.get('advance_for_booking')
            item['payment_on_event_date'] = payment_cancellation_policy.get('payment_on_event_date')
            item['payment_on_delivery'] = payment_cancellation_policy.get('payment_on_delivery')
            item['cancellation_policy'] = payment_cancellation_policy.get('cancellation_policy')
        item['is_documents_verified'] = data.get('is_documents_verified')

        yield item

if __name__ == '__main__':
    execute("scrapy crawl scrape-oppvenuz".split())