
from playwright.sync_api import sync_playwright

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto("http://localhost:8501", wait_until="networkidle")

    page.screenshot(path="jules-scratch/verification/verification_for_inspection.png")
    browser.close()

with sync_playwright() as playwright:
    run(playwright)
