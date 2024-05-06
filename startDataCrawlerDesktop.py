import pyautogui
import time
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # 스크린샷을 찍어 실시간 이미지와 비교
    current_screen_path = 'current_screen.png'
    pyautogui.screenshot(current_screen_path)

    # 이미지 탐색
    icon_location = pyautogui.locateOnScreen('C:\\Dev\\stock-api-crawling\\image\\DataCrawlerDesktop.png', confidence=0.9)
    if icon_location:
        logging.info(f"Icon found at {icon_location}. Proceeding with double-click.")
        pyautogui.doubleClick(icon_location.left, icon_location.top)
        time.sleep(1)
        pyautogui.move(0, 35)
        pyautogui.click()
    else:
        logging.error("Icon not found on the screen.")
except Exception as e:
    logging.error(f"Error during operation: {e}")
    # 실패한 경우 스크린샷 저장
    pyautogui.screenshot('error_screenshot.png')