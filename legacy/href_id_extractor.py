import re
from urllib.parse import urlparse, parse_qs

class HrefIdExtractor:
    """Extract IDs from CIAN href URLs based on patterns found in parse_search_page_data.json"""
    
    def __init__(self):
        # Regex patterns for extracting IDs
        self.patterns = {
            'district_param': re.compile(r'district%5B0%5D=(\d+)'),
            'metro_param': re.compile(r'metro%5B0%5D=(\d+)'),
            'street_param': re.compile(r'street%5B0%5D=(\d+)'),
            'room_param': re.compile(r'room(\d)=1'),
            'seo_district': re.compile(r'/snyat-.*-moskva-([a-z-]+)-(\d+)/'),
            'house_url': re.compile(r'/dom/moskva-.*-dom-.*-(\d+)/$'),
            'region_param': re.compile(r'region=(\d+)'),
            'foot_min_param': re.compile(r'foot_min=(\d+)'),
            'maxprice_param': re.compile(r'maxprice=(\d+)'),
            'currency_param': re.compile(r'currency=(\d+)'),
            'type_param': re.compile(r'type=(\d+)'),
            'offer_type_param': re.compile(r'offer_type=([a-z]+)'),
            'deal_type_param': re.compile(r'deal_type=([a-z]+)'),
            'only_foot_param': re.compile(r'only_foot=(\d+)')
        }
    
    def extract_all_ids(self, href):
        """Extract all IDs and parameters from an href"""
        results = {}
        
        # Extract district ID
        district_match = self.patterns['district_param'].search(href)
        if district_match:
            results['district_id'] = district_match.group(1)
        
        # Extract metro ID
        metro_match = self.patterns['metro_param'].search(href)
        if metro_match:
            results['metro_id'] = metro_match.group(1)
        
        # Extract street ID
        street_match = self.patterns['street_param'].search(href)
        if street_match:
            results['street_id'] = street_match.group(1)
        
        # Extract room count
        room_match = self.patterns['room_param'].search(href)
        if room_match:
            results['room_count'] = room_match.group(1)
        
        # Extract from SEO URLs
        seo_match = self.patterns['seo_district'].search(href)
        if seo_match:
            results['seo_name'] = seo_match.group(1)
            results['seo_code'] = seo_match.group(2)
        
        # Extract house ID
        house_match = self.patterns['house_url'].search(href)
        if house_match:
            results['house_id'] = house_match.group(1)
        
        # Extract other parameters
        for param_name in ['region', 'foot_min', 'maxprice', 'currency', 'type', 'offer_type', 'deal_type', 'only_foot']:
            pattern = self.patterns[f'{param_name}_param']
            match = pattern.search(href)
            if match:
                results[param_name] = match.group(1)
        
        return results
    
    def extract_district_id(self, href):
        """Extract only district ID from href"""
        match = self.patterns['district_param'].search(href)
        return match.group(1) if match else None
    
    def extract_street_id(self, href):
        """Extract only street ID from href"""
        match = self.patterns['street_param'].search(href)
        return match.group(1) if match else None
    
    def extract_metro_id(self, href):
        """Extract only metro ID from href"""
        match = self.patterns['metro_param'].search(href)
        return match.group(1) if match else None
    
    def extract_house_id(self, href):
        """Extract house ID from house URL"""
        match = self.patterns['house_url'].search(href)
        return match.group(1) if match else None
    
    def get_room_count_from_seo_url(self, href):
        """Extract room count from SEO-friendly URLs"""
        if '1-komnatnuyu' in href:
            return '1'
        elif '2-komnatnuyu' in href:
            return '2'
        elif '3-komnatnuyu' in href:
            return '3'
        elif '4-komnatnuyu' in href:
            return '4'
        elif 'studiu' in href:
            return '0'  # Studio
        return None
    
    def analyze_href_type(self, href):
        """Determine the type of href"""
        if href.startswith('https://') or href.startswith('http://'):
            return 'absolute_url'
        elif '/cat.php?' in href:
            return 'search_url'
        elif '/snyat-' in href:
            return 'seo_url'
        elif '/dom/' in href:
            return 'house_url'
        else:
            return 'unknown'
