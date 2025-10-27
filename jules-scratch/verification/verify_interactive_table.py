
from playwright.sync_api import sync_playwright

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto("http://localhost:8501")
    page.wait_for_selector("h1")

    page.screenshot(path="jules-scratch/verification/verification_before_table_click.png")

    # Click on the first row of the table
    page.click("div[data-testid='stDataFrameResizable'] div.glide-row-0")

    page.screenshot(path="jules-scratch/verification/verification_interactive.png")
    browser.close()

with sync_playwright() as playwright:
    run(playwright)
