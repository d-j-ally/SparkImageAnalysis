"""
Generate sample images for testing the Spark color analysis pipeline
Run this first to create test data
"""
from PIL import Image, ImageDraw
import os
import random

def create_test_image(filename, width=800, height=600, dominant_colors=None):
    """Create a test image with specific color distribution"""
    img = Image.new('RGB', (width, height))
    draw = ImageDraw.Draw(img)
    
    if dominant_colors is None:
        dominant_colors = [
            ('#FF0000', 0.4),  # Red - 40%
            ('#00FF00', 0.3),  # Green - 30%
            ('#0000FF', 0.2),  # Blue - 20%
            ('#FFFF00', 0.1),  # Yellow - 10%
        ]
    
    # Calculate how many pixels per color
    total_pixels = width * height
    y_offset = 0
    
    for color, percentage in dominant_colors:
        band_height = int(height * percentage)
        draw.rectangle([(0, y_offset), (width, y_offset + band_height)], fill=color)
        y_offset += band_height
    
    # Fill remaining space with last color
    if y_offset < height:
        draw.rectangle([(0, y_offset), (width, height)], fill=dominant_colors[-1][0])
    
    # Add some random noise for realism
    pixels = img.load()
    for _ in range(int(total_pixels * 0.01)):  # 1% random pixels
        x = random.randint(0, width - 1)
        y = random.randint(0, height - 1)
        r = random.randint(0, 255)
        g = random.randint(0, 255)
        b = random.randint(0, 255)
        pixels[x, y] = (r, g, b)
    
    img.save(filename)
    print(f"Created: {filename}")

def main():
    # Create output directory
    output_dir = "data/images"
    os.makedirs(output_dir, exist_ok=True)
    
    print("Generating test images...")
    print("="*60)
    
    # Image 1: Predominantly red
    create_test_image(
        f"{output_dir}/image1.jpg",
        dominant_colors=[
            ('#FF0000', 0.5),  # Red - 50%
            ('#800000', 0.3),  # Dark red - 30%
            ('#000000', 0.2),  # Black - 20%
        ]
    )
    
    # Image 2: Predominantly blue
    create_test_image(
        f"{output_dir}/image2.jpg",
        dominant_colors=[
            ('#0000FF', 0.4),  # Blue - 40%
            ('#000080', 0.3),  # Navy - 30%
            ('#FFFFFF', 0.3),  # White - 30%
        ]
    )
    
    # Image 3: Green nature-like
    create_test_image(
        f"{output_dir}/image3.jpg",
        dominant_colors=[
            ('#00FF00', 0.4),  # Green - 40%
            ('#008000', 0.3),  # Dark green - 30%
            ('#8B4513', 0.2),  # Brown - 20%
            ('#87CEEB', 0.1),  # Sky blue - 10%
        ]
    )
    
    # Image 4: Grayscale
    create_test_image(
        f"{output_dir}/image4.jpg",
        dominant_colors=[
            ('#FFFFFF', 0.3),  # White - 30%
            ('#808080', 0.3),  # Gray - 30%
            ('#404040', 0.2),  # Dark gray - 20%
            ('#000000', 0.2),  # Black - 20%
        ]
    )
    
    # Image 5: Warm colors
    create_test_image(
        f"{output_dir}/image5.jpg",
        dominant_colors=[
            ('#FFA500', 0.4),  # Orange - 40%
            ('#FFD700', 0.3),  # Gold - 30%
            ('#FF4500', 0.2),  # Red-orange - 20%
            ('#FFFF00', 0.1),  # Yellow - 10%
        ]
    )
    
    print("="*60)
    print(f"\nCreated 5 test images in {output_dir}/")

if __name__ == "__main__":
    main()