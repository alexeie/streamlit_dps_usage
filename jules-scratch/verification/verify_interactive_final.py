
from playwright.sync_api import sync_playwright

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto("http://localhost:8501", wait_until="networkidle")

    # The table is inside an iframe, so we need to switch to it
    iframe = page.frame_locator("iframe[title='streamlitApp']")

    # Selector for the first data row in the table within the iframe
    first_row_selector = "div[data-testid='stDataFrame'] tr.row-0"

    # Wait for the table to be fully loaded and visible in the iframe
    iframe.locator(first_row_selector).wait_for(state='visible')

    # Click on the first row
    iframe.locator(first_row_selector).click()

    # Wait for any potential re-rendering after the click
    page.wait_for_timeout(2000) # Increased delay for UI update

    page.screenshot(path="jules-scratch/verification/verification_interactive_final.png")
    browser.close()

with sync_playwright() as playwright:
    run(playwright)
