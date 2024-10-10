from PIL import Image
import io

def compress_image(image_data):
    image = Image.open(io.BytesIO(image_data))
    output = io.BytesIO()
    image.save(output, format='JPEG', quality=50)  # Compress image to 50%
    return output.getvalue()
