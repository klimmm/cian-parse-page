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

// Function to extract summary information from search page
async function extractSummary() {
    // First check for error pages
    checkForErrors();
    
    // Wait a bit for the page to load
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    let totalListings = null;
    let summaryText = null;
    
    // Method 1: Try to find the summary text with total count
    // Look for text pattern "Найдено X объявлений"
    const allElements = document.querySelectorAll('*');
    
    for (const element of allElements) {
        const text = element.textContent || '';
        const match = text.match(/Найдено\s+(\d+)\s+объявлени[еяй]/);
        
        if (match && element.children.length === 0) { // Only leaf nodes
            summaryText = match[0];
            totalListings = parseInt(match[1], 10);
            console.log(` Found summary: ${summaryText}`);
            break;
        }
    }
    
    // Method 2: If not found, try to find in h5 tags specifically
    if (!totalListings) {
        const h5Elements = document.querySelectorAll('h5');
        for (const h5 of h5Elements) {
            const text = h5.textContent || '';
            const match = text.match(/Найдено\s+(\d+)\s+объявлени[еяй]/);
            
            if (match) {
                summaryText = match[0];
                totalListings = parseInt(match[1], 10);
                console.log(`✅ Found summary in h5: ${summaryText}`);
                break;
            }
        }
    }
    
    // Method 3: Try to count actual offer cards as fallback
    if (!totalListings) {
        const offerCards = document.querySelectorAll('[data-name="Offers"] [data-name="CardComponent"]');
        if (offerCards.length > 0) {
            console.log(` Could not find summary text, counting cards: ${offerCards.length}`);
            // This is just cards on current page, not total
            totalListings = null; // We can't determine total from cards alone
        }
    }
    
    // Throw error if no total found, otherwise return the count
    if (!totalListings) {
        throw new Error("Could not extract total listings count from page");
    }
    
    return { listings: totalListings };
}

// Execute and return result
const result = await extractSummary();
return result;
})();