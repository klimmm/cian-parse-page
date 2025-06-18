(async () => {
    const result = {};
    
    // Check for error pages
    const errorCode = document.querySelector('h5.error-code');
    if (errorCode && (errorCode.textContent.includes('404') || errorCode.textContent.includes('Ошибка 404'))) {
        console.log('[ERROR] 404 Error page detected');
        throw new Error('404 - Page not found');
    }
    
    // Check for 404 in page title (more specific to avoid false positives)
    if (document.title && (
        document.title.includes('Ошибка 404') || 
        document.title.includes('Error 404') ||
        document.title === '404' ||
        document.title.match(/^404\s/) || // Starts with "404 "
        document.title.match(/\s404$/) || // Ends with " 404"
        document.title.match(/\s404\s/) // Has " 404 " in the middle
    )) {
        console.log('[ERROR] 404 detected in page title: ' + document.title);
        throw new Error('404 - Page not found');
    }
    
    // Check for "Страница не найдена" in h1
    const errorPageTitle = document.querySelector('h1.title');
    if (errorPageTitle && errorPageTitle.textContent.includes('Страница не найдена')) {
        console.log('[ERROR] 404 page detected by title text');
        throw new Error('404 - Page not found');
    }
    
    // Check for 429 rate limit error
    const headerCode = document.querySelector('.header__code');
    if (headerCode && headerCode.textContent.trim() === '429') {
        console.log('[WARN] 429 Rate limit error detected');
        throw new Error('429 - Too many requests');
    }
    
    // Check for rate limit in page title (exact match)
    const pageTitle = document.title;
    if (pageTitle && (pageTitle.trim() === '429' || pageTitle.includes('429 '))) {
        console.log('[WARN] 429 Rate limit detected in page title');
        throw new Error('429 - Too many requests');
    }
    
    // Check for "too many requests" message
    const bodyText = document.body.textContent;
    if (bodyText && bodyText.toLowerCase().includes('too many requests')) {
        console.log('[WARN] Rate limit message detected in page');
        throw new Error('429 - Too many requests');
    }
    
    console.log('[INFO] Parsing complete CIAN property data...\n');
    
    // Wait for page content to load before parsing
    // Helper function to wait for element with optimized polling
    const waitForElement = (selector, timeout = 5000) => {
        return new Promise((resolve) => {
            const element = document.querySelector(selector);
            if (element) {
                resolve(element);
                return;
            }
            
            const startTime = Date.now();
            const pollInterval = 100; // Check every 100ms
            
            const poll = () => {
                const element = document.querySelector(selector);
                if (element) {
                    resolve(element);
                    return;
                }
                
                if (Date.now() - startTime >= timeout) {
                    resolve(null);
                    return;
                }
                
                setTimeout(poll, pollInterval);
            };
            
            setTimeout(poll, pollInterval);
        });
    };
    
    // Wait for OfferValuationContainerLoader (primary target)
    console.log('[SEARCH] Waiting for OfferValuationContainerLoader...');
    const priceLoader = await waitForElement('[data-name="OfferValuationContainerLoader"]', 5000);
    
    if (priceLoader) {
        console.log('[OK] OfferValuationContainerLoader found');
    } else {
        console.log('[WARN] OfferValuationContainerLoader not found, checking fallbacks...');
        
        // Fallback: wait for Geo or Metadata sections
        const fallbackElement = await waitForElement('[data-name="Geo"], [data-name="OfferMetaData"]', 3000);
        
        if (fallbackElement) {
            console.log(`[OK] Fallback element found: ${fallbackElement.getAttribute('data-name')}`);
        } else {
            console.log('[WARN] No key elements found');
            throw new Error('No key elements found');
        }
    }
    
    // Extract offer_id from URL - the URL is added by scraper, not window.location
    // Get offer_id from current page URL (passed by scraper)
    // Since this runs in page context, we need to extract from the actual browser URL
    const pageUrl = window.location.href;
    const offerIdMatch = pageUrl.match(/\/rent\/flat\/(\d+)/);
    if (offerIdMatch) {
        result.offer_id = offerIdMatch[1];
        console.log(`[OK] Extracted offer_id: ${result.offer_id} from ${pageUrl}`);
    } else {
        console.log(`[ERROR] Failed to extract offer_id from URL: ${pageUrl}`);
    }
    
    // 1. Check if offer is unpublished
    const unpublishedContainer = document.querySelector('[data-name="OfferUnpublished"]');
    result.isUnpublished = !!unpublishedContainer;
    
    if (unpublishedContainer) {
        console.log('[ERROR] Offer is unpublished - skipping estimation');
    } else {
        console.log('[OK] Offer is published');
    }
    
    // 2. Parse Cian estimation price (with lazy loading) - only for published offers
    if (!unpublishedContainer) {
        const valuationContainerLoader = document.querySelector('[data-name="OfferValuationContainerLoader"]');
        
        if (valuationContainerLoader) {
        // Scroll to the element to trigger lazy loading
        valuationContainerLoader.scrollIntoView({ behavior: 'instant', block: 'center' });
        
        // Wait for the valuation container to appear (with timeout)
        const valuationContainer = await waitForElement('[data-name="OfferValuationContainer"]', 10000);
        
        if (valuationContainer) {
            const estimationPrice = valuationContainer.querySelector('[data-testid="valuation_estimationPrice"] span')?.textContent?.trim();
            const offerPrice = valuationContainer.querySelector('[data-testid="valuation_offerPrice"] span')?.textContent?.trim();
            
            if (estimationPrice) {
                result['estimation_price'] = estimationPrice;
                console.log(`[OK] Cian estimation: ${estimationPrice}`);
            } else {
                console.log('[ERROR] Cian estimation not found in container');
            }
            
            if (offerPrice) {
                // Add dot to match search results format
                result['offer_price'] = offerPrice.endsWith('.') ? offerPrice : offerPrice + '.';
                console.log(`[OK] Listed price: ${result['offer_price']}`);
            }
        } else {
            console.log('[ERROR] OfferValuationContainer not loaded after scroll');
            
            // Fallback: try to get price from PriceInfo
            const priceInfo = document.querySelector('[data-testid="price-amount"]');
            if (priceInfo) {
                const fallbackPrice = priceInfo.textContent?.trim();
                if (fallbackPrice) {
                    result['offerPrice'] = fallbackPrice;
                    console.log(`[OK] Fallback price from PriceInfo: ${fallbackPrice}`);
                }
            } else {
                console.log('[ERROR] Fallback price not found');
            }
            
            console.log('[ERROR] Published offer failed to load Cian estimation');
        }
        } else {
            console.log('[ERROR] OfferValuationContainerLoader not found');
            // Fallback: try to get price from PriceInfo
            const priceInfo = document.querySelector('[data-testid="price-amount"]');
            if (priceInfo) {
                const fallbackPrice = priceInfo.textContent?.trim();
                if (fallbackPrice) {
                    result['offerPrice'] = fallbackPrice;
                    console.log(`[OK] Fallback price from PriceInfo: ${fallbackPrice}`);
                }
            } else {
                console.log('[ERROR] Fallback price not found');
            }
            
            // Scroll to FeaturesLayout as backup
            const featuresContainer = document.querySelector('[data-name="FeaturesLayout"]');
            if (featuresContainer) {
                featuresContainer.scrollIntoView({ behavior: 'instant', block: 'center' });
                console.log('[ACTION] Scrolled to FeaturesLayout as backup');
            }
        }
    } else {
        console.log('[SKIP] Skipping estimation parsing for unpublished offer');
        
        // For unpublished offers, still try to get the price
        const priceInfo = document.querySelector('[data-testid="price-amount"]');
        if (priceInfo) {
            const fallbackPrice = priceInfo.textContent?.trim();
            if (fallbackPrice) {
                result['offer_price'] = fallbackPrice;
                console.log(`[OK] Price from unpublished offer: ${fallbackPrice}`);
            }
        } else {
            console.log('[ERROR] Price not found for unpublished offer');
        }
    }
    
    // 3. Parse features
    const featuresContainer = document.querySelector('[data-name="FeaturesLayout"]');
    if (featuresContainer) {
        const items = featuresContainer.querySelectorAll('[data-name="FeaturesItem"]');
        const features = Array.from(items).map(item => item.textContent.trim());
        result['features'] = features;
        console.log(`[OK] Features: ${features.length} items`);
    } else {
        console.log('[ERROR] Features not found');
    }
    
    // 4. Parse offer summary
    const offerContainer = document.querySelector('[data-name="OfferSummaryInfoLayout"]');
    if (offerContainer) {
        const groups = offerContainer.querySelectorAll('[data-name="OfferSummaryInfoGroup"]');
        
        groups.forEach((group, index) => {
            const title = group.querySelector('h2')?.textContent?.trim();
            const isApartmentSection = title === 'О квартире' || index === 0;
            const sectionName = isApartmentSection ? 'apartment' : 'building';
            const sectionData = {};
            
            const items = group.querySelectorAll('[data-name="OfferSummaryInfoItem"]');
            items.forEach(item => {
                const [label, value] = Array.from(item.querySelectorAll('p')).map(p => p.textContent.trim());
                if (label && value) sectionData[label] = value;
            });
            
            result[sectionName] = sectionData;
        });
        
        console.log(`[OK] Property details: ${groups.length} sections`);
    } else {
        console.log('[ERROR] Property details not found');
    }


    // 5. Parse metadata
    const metadataContainer = document.querySelector('[data-name="OfferMetaData"]');
    if (metadataContainer) {
        const metadata = {};
        
        // Update date
        const updateDate = metadataContainer.querySelector('[data-testid="metadata-updated-date"] span')?.textContent?.trim();
        if (updateDate) {
            metadata.updated_date = updateDate.replace('Обновлено: ', '');
        }
        
        // Stats
        const statsButton = metadataContainer.querySelector('[data-name="OfferStats"]');
        if (statsButton) {
            const statsText = statsButton.textContent?.trim();
            metadata.offer_stats = statsText;
        }
        
        // Published status
        metadata.is_unpublished = result.isUnpublished;
        delete result.isUnpublished;
        
        result['metadata'] = metadata;
        console.log(`[OK] Metadata: ${Object.keys(metadata).length} items`);
    } else {
        console.log('[ERROR] Metadata not found');
    }
    
    // 6. Parse location (Geo)
    const geoContainer = document.querySelector('[data-name="Geo"]');
    if (geoContainer) {
        const location = {};
        
        const fullAddress = geoContainer.querySelector('[itemprop="name"]')?.getAttribute('content');
        if (fullAddress) location.full_address = fullAddress;
        
        const addressItems = geoContainer.querySelectorAll('[data-name="AddressItem"]');
        if (addressItems.length > 0) {
            const addressParts = Array.from(addressItems).map(item => {
                const text = item.textContent?.trim();
                const href = item.href;
                return { text, href };
            }).filter(item => item.text);
            location['address_items'] = addressParts;
        }
        
        const metroStations = geoContainer.querySelectorAll('[data-name="UndergroundItem"]');
        if (metroStations.length > 0) {
            const metro = Array.from(metroStations).map(station => {
                const linkElement = station.querySelector('a');
                const name = linkElement?.textContent?.trim();
                // const href = linkElement?.href;
                
                // The time is in a nested structure, look for text containing "мин."
                const timeText = station.textContent?.match(/\d+\s*мин\./)?.[0];
                
                return name ? { name, walking_time: timeText || null } : null;
            }).filter(Boolean);
            location.metro_stations = metro;
        }
        
        result['geo'] = location;
        console.log(`[OK] Location: address + ${addressItems.length} address parts + ${metroStations.length} metro stations`);
    } else {
        console.log('[ERROR] Location not found');
    }
    
    // 7. Parse object factoids (key property facts) and merge with apartment
    const factoidsContainer = document.querySelector('[data-name="ObjectFactoids"]');
    if (factoidsContainer) {
        const factoidItems = factoidsContainer.querySelectorAll('[data-name="ObjectFactoidsItem"]');
        let addedCount = 0;
        
        // Ensure apartment section exists
        if (!result.apartment) result.apartment = {};
        
        factoidItems.forEach(item => {
            const spans = item.querySelectorAll('span');
            if (spans.length >= 2) {
                const label = spans[0].textContent?.trim();
                const value = spans[1].textContent?.trim();
                
                if (label && value) {
                    // Check if this key-value already exists in apartment or building sections
                    const existsInApartment = result.apartment && result.apartment[label] === value;
                    const existsInBuilding = result.building && result.building[label] === value;
                    
                    if (!existsInApartment && !existsInBuilding) {
                        result.apartment[label] = value;
                        addedCount++;
                    }
                }
            }
        });
        
        console.log(`[OK] Key facts: ${addedCount} unique items added to apartment section`);
    } else {
        console.log('[ERROR] Key facts not found');
    }
    
    // 8. Parse description
    const descriptionContainer = document.querySelector('[data-name="Description"]');
    if (descriptionContainer) {
        const descriptionText = descriptionContainer.querySelector('span')?.textContent?.trim();
        if (descriptionText) {
            result.description = descriptionText;
            console.log(`[OK] Description: ${descriptionText.length} characters`);
        }
    } else {
        console.log('[ERROR] Description not found');
    }
    
    // 9. Parse offer facts (rental terms)
    const offerFactsContainer = document.querySelector('[data-name="OfferFactsInSidebar"]');
    if (offerFactsContainer) {
        const rentalTerms = {};
        const factItems = offerFactsContainer.querySelectorAll('[data-name="OfferFactItem"]');
        factItems.forEach(item => {
            const spans = item.querySelectorAll('span');
            if (spans.length >= 2) {
                const label = spans[0].textContent?.trim();
                const value = spans[spans.length - 1].textContent?.trim(); // Last span contains the value
                if (label && value && label !== value) rentalTerms[label] = value;
            }
        });
        result['rental_terms'] = rentalTerms;
        console.log(`[OK] Rental terms: ${factItems.length} items`);
    } else {
        console.log('[ERROR] Rental terms not found');
    }
    
    // Add timestamp
    result.timestamp = new Date().toISOString();
    
    // Reorder keys in the desired sequence
    const orderedResult = {
        offer_id: result.offer_id,
        offer_price: result.offer_price || result.offerPrice,
        estimation_price: result.estimation_price,
        metadata: result.metadata,
        geo: result.geo,
        rental_terms: result.rental_terms,
        apartment: result.apartment,
        building: result.building,
        features: result.features,
        description: result.description,
        timestamp: result.timestamp
    };
    
    // Remove any undefined values to keep the object clean
    Object.keys(orderedResult).forEach(key => {
        if (orderedResult[key] === undefined) {
            delete orderedResult[key];
        }
    });
    
    // Validate that offer_price was found
    if (!orderedResult.offer_price) {
        console.log('[ERROR] Critical error: offer_price not found in parsing results');
        throw new Error('offer_price not found - required field missing');
    }
    
    console.log('\n[DATA] Complete parsed data:');
    console.log(orderedResult);
    
    return orderedResult;
})();