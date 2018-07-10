"""Tools for processing images.

"""

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np


def img_tile(img, n, m=1):
    """The image "img" will be repeated n times in vertical and m times in 
    horizontal direction.
    
    """
    if n == 1:
        tiled_img = img
    else:
        lst_imgs = []
        for _ in range(n):
            lst_imgs.append(img)  
        tiled_img = np.concatenate(lst_imgs, axis=1 )
    if m > 1:
        lst_imgs = []
        for _ in range(m):
            lst_imgs.append(tiled_img)  
        tiled_img = np.concatenate(lst_imgs, axis=0 )
          
    return tiled_img


if __name__ == '__main__':
    basic_pattern = mpimg.imread('c:\\aaa\\decorators_b2.png')
    print(basic_pattern)
    decorators_img = img_tile(basic_pattern, 3, 3)
    plt.axis("off")
    plt.imshow(decorators_img)
    plt.show()