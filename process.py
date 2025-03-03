import logging
from pathlib import Path
import time
from prefect import flow, task
from PIL import Image
from config import FORMAT, INPUT_DIR, OUTPUT_DIR, RGB2XYZ, RGB_L

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format=FORMAT)


@task
def load_image():
    return list(Path(INPUT_DIR).glob("*.jpg"))

@task
def process_image(image_path):
    try:
        image = Image.open(image_path)
        image = image.convert(RGB_L, RGB2XYZ)
        output_path = Path(OUTPUT_DIR) / image_path.name
        image.save(output_path, format="JPEG", quality=85)  #No Compression
        return f"Processed: {image_path.name}"
    except Exception as e:
        return f"Error processing {image_path.name}: {e}"

@task
def log_results(results):
    for result in results:
        logging.info(result)
        print(result)

@flow(name="parallel image processing")
def transform_image():
    images = load_image()
    print(images)
    results = process_image.map(images) # for parallel excecution of tasks
    log_results(results)

if __name__== "__main__" :
    start_time = time.time()
    transform_image()
    print(f"Total Processing Time: {time.time() - start_time:.2f} seconds")
