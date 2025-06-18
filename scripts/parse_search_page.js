(async () => {
'use strict';

// Check for error pages before parsing
function checkForErrors() {
    // Check for 429 rate limit
    const headerCode = document.querySelector('.header__code');
    if (headerCode && headerCode.textContent === '429') {
        throw new Error('429 - Too many requests');
    }
    
    // Check for rate limit in page title
    const pageTitle = document.title;
    if (pageTitle && pageTitle.includes('429')) {
        throw new Error('429 - Too many requests');
    }
    
    // Check for "too many requests" message
    const bodyText = document.body.textContent;
    if (bodyText && bodyText.toLowerCase().includes('too many requests')) {
        throw new Error('429 - Too many requests');
    }
    
    // Check for 404 error
    const errorCode404 = document.querySelector('h5.error-code');
    if (errorCode404 && errorCode404.textContent.includes('404')) {
        throw new Error('404 - Page not found');
    }
}

// Wait for search page content to load before parsing
async function waitForSearchContent() {
    let searchReady = false;
    let attempts = 0;
    const maxAttempts = 50; // 5 seconds max (50 * 100ms)
    
    console.log('🔍 Waiting for search results to load...');
    while (!searchReady && attempts < maxAttempts) {
        const offersContainer = document.querySelector('[data-name="Offers"]');
        const cardComponents = document.querySelectorAll('[data-name="Offers"] [data-name="CardComponent"]');
        
        if (offersContainer && cardComponents.length > 0) {
            searchReady = true;
            console.log(`✅ Search results loaded: ${cardComponents.length} cards found after ${attempts * 100}ms`);
        } else {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
    }
    
    if (!searchReady) {
        console.log('⚠️ Search results may not be fully loaded');
    }
    
    return searchReady;
}

// Function to extract all available information from card elements
async function extractCardData() {
    // First check for error pages
    checkForErrors();
    
    // Wait for content to load first
    const contentLoaded = await waitForSearchContent();
    
    // If content didn't load, throw error
    if (!contentLoaded) {
        console.log('❌ Failed to load search results, throwing error');
        throw new Error('Search results may not be fully loaded');
    }
    // Use CardComponent inside Offers container to find all cards
    const cards = document.querySelectorAll('[data-name="Offers"] [data-name="CardComponent"]');
    const results = [];
    
    cards.forEach((card, index) => {
        // Look for the link within each card
        const link = card.querySelector('a[href*="/rent/flat/"]');
        
        if (link) {
            const url = link.href.replace(/\/$/, '');
            // Extract offer_id from URL
            const match = url.match(/\/rent\/flat\/(\d+)\/?/);
            const offerId = match ? match[1] : null;
            
            // Extract main price
            let offer_price = null;
            const priceElement = card.querySelector('[data-mark="MainPrice"]');
            if (priceElement) {
                const priceText = priceElement.textContent.trim();
                offer_price = priceText;
            }
            
            // Extract additional price info
            let priceInfo = null;
            const priceInfoElement = card.querySelector('[data-mark="PriceInfo"]');
            if (priceInfoElement) {
                priceInfo = priceInfoElement.textContent.trim();
            }
            
            // Extract time label - only absolute time
            let timeLabel = null;
            const timeLabelElement = card.querySelector('[data-name="TimeLabel"]');
            if (timeLabelElement) {
                // Look for the absolute time in the specific div
                const absoluteTimeDiv = timeLabelElement.querySelector('._93444fe79c--absolute--yut0v');
                if (absoluteTimeDiv) {
                    const timeSpan = absoluteTimeDiv.querySelector('span');
                    if (timeSpan) {
                        timeLabel = timeSpan.textContent.trim();
                    }
                }
            }
            
            // Extract title - prefer OfferSubtitle if it exists, otherwise use OfferTitle
            let title = null;
            const subtitleElement = card.querySelector('[data-mark="OfferSubtitle"]');
            if (subtitleElement) {
                const subtitleSpan = subtitleElement.querySelector('span');
                if (subtitleSpan) {
                    title = subtitleSpan.textContent.trim();
                }
            }
            
            // If no title from subtitle, try OfferTitle
            if (!title) {
                const titleElement = card.querySelector('[data-mark="OfferTitle"]');
                if (titleElement) {
                    const titleSpan = titleElement.querySelector('span');
                    if (titleSpan) {
                        title = titleSpan.textContent.trim();
                    }
                }
            }
            
            // Metro station will be extracted from geoLabels
            
            // Extract geo information
            let fullAddress = null;
            let addressItems = [];
            let metroStations = [];

            const geoLabels = card.querySelectorAll('[data-name="GeneralInfoSectionRowComponent"] [data-name="GeoLabel"]');
            const geoTexts = [];

            geoLabels.forEach((label) => {
                const text = label.textContent.trim();
                if (text) {
                    geoTexts.push(text);
                }
            });
            
            fullAddress = geoTexts.join(', ');
            
            geoLabels.forEach((label) => {
                const text = label.textContent.trim();
                const href = label.getAttribute('href');

                if (!href || !text) return;
                
                // Parse URL parameters
                const url = new URL(href, window.location.origin);
                const params = url.searchParams;
                
                // Check if it's a metro station
                if (params.has('metro[0]')) {
                    metroStations.push({
                        name: text.replace(/^м\.\s*/, ''),
                        href: href
                    });
                } else {
                    // Add all non-metro items to address_items
                    addressItems.push({
                        text: text,
                        href: href
                    });
                }
            });
            
            // Parse price info into separate fields
            let rentalPeriod = null;
            let utilitiesIncluded = null;
            let commission = null;
            let deposit = null;
            
            if (priceInfo) {
                const parts = priceInfo.split(',').map(part => part.trim());
                
                // Process each part and extract key-value pairs based on position
                if (parts[0]) {
                    rentalPeriod = parts[0].toLowerCase().replace(/^на\s+/, '');
                }
                
                if (parts[1]) {
                    utilitiesIncluded = parts[1].toLowerCase()
                        .replace('комм. платежи', '')
                        .replace('включены', 'включена')
                        .trim();
                }
                
                // For commission: extract everything after first word, except special cases
                if (parts[2]) {
                    const commissionText = parts[2].toLowerCase().trim();
                    if (commissionText.includes('без комиссии') || commissionText.includes('комиссии нет')) {
                        commission = 'нет';
                    } else {
                        const commissionParts = commissionText.split(' ');
                        commission = commissionParts.length > 1 ? commissionParts.slice(1).join(' ') : commissionText;
                    }
                }
                
                // For deposit: extract everything after first word, except special cases
                if (parts[3]) {
                    const depositText = parts[3].toLowerCase().trim();
                    if (depositText.includes('без залога') || depositText.includes('залога нет')) {
                        deposit = 'нет';
                    } else {
                        const depositParts = depositText.split(' ');
                        deposit = depositParts.length > 1 ? depositParts.slice(1).join(' ') : depositText;
                    }
                }
            }
            
            // Extract description
            let description = null;
            const descElement = card.querySelector('[data-name="Description"]');
            if (descElement) {
                description = descElement.textContent.trim();
            }
            
            // Extract image URLs from Gallery
            let imageUrls = [];
            const galleryElement = card.querySelector('[data-name="Gallery"]');
            if (galleryElement) {
                const imgElements = galleryElement.querySelectorAll('img[src*="cdn-cian.ru"]');
                imgElements.forEach(img => {
                    let imgUrl = img.getAttribute('src');
                    if (imgUrl && imgUrl.includes('cdn-cian.ru')) {
                        // Replace -4.jpg with -1.jpg
                        imgUrl = imgUrl.replace(/-4\.jpg$/, '-1.jpg');
                        imageUrls.push(imgUrl);
                    }
                });
            }
            
            // Parse apartment details from title
            let apartment = {};
            if (title) {
                // Smart split: avoid splitting commas inside numbers like "6,6"
                // Use regex to split on commas that are not between digits
                const titleParts = title.split(/,(?!\d)/).map(part => part.trim());
                
                // Second part should be area (Общая площадь)
                if (titleParts[1]) {
                    apartment['Общая площадь'] = titleParts[1];
                }
                
                // Third part should be floor (Этаж)
                if (titleParts[2]) {
                    apartment['Этаж'] = titleParts[2].replace('/', ' из ').replace(/\s*этаж\s*/i, '');
                }
            }
            
            const timestamp = new Date().toISOString();
            
            if (offerId) {
                results.push({
                    offer_id: offerId,
                    title: title,                    
                    offer_price: offer_price,
                    metadata: {
                        updated_date: timeLabel
                    },                    
                    geo: {
                        full_address: fullAddress,
                        address_items: addressItems
                        // metro_stations: metroStations
                    },
                    rental_terms: {
                        "Срок аренды": rentalPeriod,
                        "Оплата ЖКХ": utilitiesIncluded,
                        "Комиссия": commission,
                        "Залог": deposit,
                    },
                    apartment: apartment,
                    description: description,
                    image_urls: imageUrls.join(','),
                    timestamp: timestamp,
                    url: url
                    // price_info: priceInfo
                });
            }
        }
    });
    
    return results;
}

// Return dictionary with comprehensive offer information
const result = await extractCardData();

// Wrap the array of results in a dictionary format for unified handling
return {
    search_results: result,
    total_found: result.length,
    timestamp: new Date().toISOString()
};
})();
