import os
import sys
import time
from pathlib import Path
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

# Add project root to sys.path to allow standalone execution
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from core.utils.helpers import (
    polite_sleep, normalize_date_column, save_dataframe,
    _try_click_any, setup_driver, SAVE_FORMAT, OUTPUT_BASE_DIR
)


def accept_cookies_wisdomtree(driver):
    """Handle cookie consent banner and welcome modals on the WisdomTree website."""
    # 1. Cookie Consent
    _ = _try_click_any(driver, [
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "//a[@id='CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll']",
        "//button[@id='CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll']",
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow all')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow all')]",
    ], wait_sec=10)
    polite_sleep()

    # 2. Welcome Modal / Country Selection
    _try_click_any(driver, [
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue to us website')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue to us website')]",
        "a.button-link.login-btn.button.-secondary.continue-btn",
        "a.button.-secondary.continue-btn",
    ], wait_sec=8)
    polite_sleep()

    # 3. Investor Type Selection (if prompted to avoid Restricted Content)
    try:
        investor_choices = [
            "//span[contains(text(),'Individual Investor')]",
            "//button[contains(.,'Individual Investor')]",
            "//a[contains(.,'Individual Investor')]",
            "//span[contains(text(),'Financial Professional')]",
            "//button[contains(.,'Financial Professional')]"
        ]
        if _try_click_any(driver, investor_choices, wait_sec=5):
            print("[WISDOMTREE] Selected investor type.")
            _try_click_any(driver, ["//button[contains(.,'Enter Site') or contains(.,'Confirm') or contains(.,'Accept')]"], wait_sec=3)
            polite_sleep()
    except:
        pass


def _wisdomtree_open_history_modal(driver):
    """Navigate and open the Premium/Discount History modal on the WisdomTree page."""
    # Scroll in steps to ensure dynamic elements load and avoid detection
    for i in range(5):
        driver.execute_script(f"window.scrollBy(0, {1500});")
        time.sleep(0.5)

    # Pre-emptively intercept modal close events via JS
    driver.execute_script("""
        // Intercept Bootstrap modal hide event to prevent auto-close
        if (window.jQuery) {
            jQuery(document).on('hide.bs.modal', '.modal', function(e) {
                console.log('[WISDOMTREE SCRAPER] Prevented modal close event.');
                e.preventDefault();
                e.stopPropagation();
                return false;
            });
        }
        // Block close button clicks temporarily as well
        const closeButtons = document.querySelectorAll('.modal .close, .modal [data-dismiss="modal"]');
        closeButtons.forEach(btn => btn.style.pointerEvents = 'none');
    """)
    time.sleep(0.3)

    # Specific search for 'View NAV History' or 'Premium/Discount History' link
    link_selectors = [
        "a.fund-modal-trigger[data-href*='nav-premium-discount-history']",
        "//a[contains(@class,'fund-modal-trigger') and contains(.,'View NAV History')]",
        "//a[contains(@class,'fund-modal-trigger') and contains(.,'Premium/Discount History')]",
        "//a[contains(@data-href,'nav-premium-discount-history')]",
        "a[data-href*='nav-premium-discount-history']"
    ]

    link_found = False
    for sel in link_selectors:
        try:
            by = By.CSS_SELECTOR if not sel.startswith("//") else By.XPATH
            link = WebDriverWait(driver, 10).until(EC.presence_of_element_located((by, sel)))
            
            # Scroll without smooth behavior to avoid timing issues
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            time.sleep(1.5)  # Give page time to settle
            
            # Use ActionChains to simulate human-like mouse movement and click
            actions = ActionChains(driver)
            actions.move_to_element(link).pause(0.5).click().perform()
            
            link_found = True
            print(f"[WISDOMTREE] Triggered modal via ActionChains: {sel}")
            break
        except Exception as ex:
            print(f"[WISDOMTREE] Selector failed: {sel} -> {ex}")
            continue

    if not link_found:
        # Last resort: try clicking anything with fund-modal-trigger if it has the right text
        try:
            extras = driver.find_elements(By.CLASS_NAME, "fund-modal-trigger")
            for e in extras:
                if "NAV" in e.text or "History" in e.text:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", e)
                    time.sleep(0.5)
                    ActionChains(driver).move_to_element(e).pause(0.3).click().perform()
                    link_found = True
                    break
        except: pass

    if not link_found:
        raise RuntimeError("WisdomTree: Modal history link not found after scrolling.")

    # Wait for modal container to appear
    print("[WISDOMTREE] Waiting for modal to appear...")
    WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "div.modal.fade.in, div.modal.show, .modal.in"))
    )
    print("[WISDOMTREE] Modal appeared!")

    # STABILITY HACK: Disable backdrop and keyboard closing via JS
    driver.execute_script("""
        const modals = document.querySelectorAll('.modal');
        modals.forEach(m => {
            if (window.jQuery && jQuery(m).data('bs.modal')) {
                jQuery(m).data('bs.modal').options.backdrop = 'static';
                jQuery(m).data('bs.modal').options.keyboard = false;
            }
            // Also block backdrop pointer events
            m.style.pointerEvents = 'auto';
        });
        const backdrops = document.querySelectorAll('.modal-backdrop');
        backdrops.forEach(b => {
            b.style.pointerEvents = 'none';
            b.onclick = function(e) { e.stopPropagation(); e.preventDefault(); return false; };
        });
    """)

    # Wait for table rows to actually appear inside the modal
    time.sleep(3.0)  # Extra long wait to ensure data loads
    WebDriverWait(driver, 25).until(
        EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'modal')]//table//tr[td]"))
    )
    print("[WISDOMTREE] Table rows detected in modal.")


def _wisdomtree_parse_table(driver):
    """Parse the data table within the WisdomTree modal into a clean DataFrame."""
    # Ensure the modal is still open
    try:
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "div.modal.fade.in, div.modal.show, .modal.in"))
        )
    except:
        print("[WISDOMTREE WARNING] Modal seems to have closed, attempting one-time recovery check...")
    
    # Target the modal table rows specifically
    rows = driver.find_elements(By.XPATH, "//div[contains(@class,'modal')]//table//tr")
    if not rows or len(rows) < 2: 
        raise RuntimeError("WisdomTree: Modal table not found or empty")
    
    data = []
    for r in rows:
        cols = r.find_elements(By.XPATH, ".//td")
        if len(cols) < 3: continue
        # Extract text: Date, NAV, Market Price
        t1, t2, t3 = cols[0].text.strip(), cols[1].text.strip(), cols[2].text.strip()
        if not t1 or "Date" in t1: continue
        data.append([t1, t2, t3])
    
    if not data: 
        raise RuntimeError("WisdomTree: Could not parse any data rows from modal")
    
    df = pd.DataFrame(data, columns=["date", "nav", "market price"])
    df = normalize_date_column(df)
    for c in ["nav", "market price"]:
        df[c] = (df[c].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip())
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    print(f"[WISDOMTREE] Parsed {len(df)} rows from modal table")
    return df[["date", "nav", "market price"]]


def process_single_etf_wisdomtree(driver, etf, site_url):
    """
    Main process to scrape a single WisdomTree ETF.
    
    IMPORTANT: This function ignores the passed driver and creates its own
    undetected-chromedriver instance to bypass Cloudflare bot detection.
    """
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (WisdomTree – Modal NAV/Px/Discount) → output .{SAVE_FORMAT}")
    print("=" * 50)
    
    # WisdomTree requires undetected-chromedriver to bypass Cloudflare
    print("[WISDOMTREE] Creating dedicated undetected-chromedriver instance...")
    
    dedicated_driver = None
    try:
        import undetected_chromedriver as uc
        
        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        # Detect headless mode from environment
        headless = os.environ.get("DISPLAY") is None or os.environ.get("ETF_HEADLESS", "false").lower() == "true"
        
        dedicated_driver = uc.Chrome(options=options, headless=headless, use_subprocess=True)
        print(f"[WISDOMTREE] ✅ undetected-chromedriver initialized (headless={headless})")
        
        dedicated_driver.get(site_url)
        polite_sleep()
        
        accept_cookies_wisdomtree(dedicated_driver)
        polite_sleep()
        
        # Diagnostic: Screen after cookies
        shot_path = os.path.join(OUTPUT_BASE_DIR, "debug_wisdomtree_after_cookies.png")
        dedicated_driver.save_screenshot(shot_path)
        print(f"[WISDOMTREE] Screenshot after cookies: {shot_path}")
        
        _wisdomtree_open_history_modal(dedicated_driver)
        df = _wisdomtree_parse_table(dedicated_driver)
        
        save_dataframe(df, base, sheet_name="Historical")
        print(f"[SUCCESS] ✓ WisdomTree processed ({name})")
        return True, None
        
    except ImportError:
        msg = "undetected-chromedriver not installed. Run: pip install undetected-chromedriver"
        print(f"[WISDOMTREE ERROR] {msg}")
        return False, msg
    except Exception as e:
        msg = f"Error: {e}"
        print(f"[WISDOMTREE ERROR] {msg}")
        # Diagnostic: Screen on error
        try:
            shot_err = os.path.join(OUTPUT_BASE_DIR, f"debug_wisdomtree_error_{int(time.time())}.png")
            dedicated_driver.save_screenshot(shot_err)
            print(f"[WISDOMTREE] Error screenshot saved: {shot_err}")
        except: pass
        return False, msg
    finally:
        if dedicated_driver:
            try:
                dedicated_driver.quit()
                print("[WISDOMTREE] Dedicated driver closed")
            except:
                pass


def main():
    """Standalone execution for WisdomTree scraper using undetected-chromedriver."""
    try:
        import undetected_chromedriver as uc
    except ImportError:
        print("[ERROR] undetected-chromedriver not installed.")
        print("Please run: pip install undetected-chromedriver")
        return

    etf = {"name": "WisdomTree Bitcoin Fund (BTCW)", "output_filename": "btcw_dailynav.xlsx"}
    site_url = "https://www.wisdomtree.com/investments/etfs/crypto/btcw"
    
    print("[WISDOMTREE] Launching undetected Chrome browser...")
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, use_subprocess=True)
    
    try:
        driver.get(site_url)
        polite_sleep()
        accept_cookies_wisdomtree(driver)
        polite_sleep()
        _wisdomtree_open_history_modal(driver)
        df = _wisdomtree_parse_table(driver)
        
        base = os.path.splitext(etf["output_filename"])[0]
        save_dataframe(df, base, sheet_name="Historical")
        print("[STANDALONE] WisdomTree processed successfully.")
    except Exception as e:
        print(f"[STANDALONE] WisdomTree failed: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
