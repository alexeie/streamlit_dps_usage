from playwright.sync_api import Page, expect
import traceback

def test_streamlit_app(page: Page):
    """
    This test verifies the changes made to the Streamlit app.
    """
    print("Running verification script...")
    try:
        # 1. Arrange: Go to the Streamlit app.
        page.goto("http://localhost:8501")

        # Wait for the page to load
        expect(page.get_by_text("Data Product Usage Dashboard")).to_be_visible()

        # 2. Act: Select the "Last 14 Days" radio button.
        page.get_by_text("Last 14 Days").click()

        # 3. Assert: Check that the graph titles are correct.
        expect(page.get_by_text("Top 10 Used Data Products (by 14-day usage)")).to_be_visible()
        expect(page.get_by_text("Top 10 Usage Comparison")).to_be_visible()

        # 4. Screenshot: Capture the final result for visual verification.
        page.screenshot(path="jules-scratch/verification/verification.png")
        print("Screenshot created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        test_streamlit_app(page)
        browser.close()
