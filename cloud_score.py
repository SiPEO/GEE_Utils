import ee

from .helpers import rescale, dilatedErossion

def sentinel2CloudScore(img):
    toa = img.select(['B1','B2','B3','B4','B5','B6','B7','B8','B8A', 'B9','B10', 'B11','B12']) \
              .divide(10000)

    toa = toa.addBands(img.select(['QA60']))

    # ['QA60', 'B1','B2',    'B3',    'B4',   'B5','B6','B7', 'B8','  B8A', 'B9',          'B10', 'B11','B12']
    # ['QA60','cb', 'blue', 'green', 'red', 're1','re2','re3','nir', 'nir2', 'waterVapor', 'cirrus','swir1', 'swir2'])

    # Compute several indicators of cloudyness and take the minimum of them.
    score = ee.Image(1)

    # Clouds are reasonably bright in the blue and cirrus bands.
    score = score.min(rescale(toa, 'img.B2', [0.1, 0.5]))
    score = score.min(rescale(toa, 'img.B1', [0.1, 0.3]))
    score = score.min(rescale(toa, 'img.B1 + img.B10', [0.15, 0.2]))

    # Clouds are reasonably bright in all visible bands.
    score = score.min(rescale(toa, 'img.B4 + img.B3 + img.B2', [0.2, 0.8]))

    #Clouds are moist
    ndmi = img.normalizedDifference(['B8','B11'])
    score=score.min(rescale(ndmi, 'img', [-0.1, 0.1]))

    # However, clouds are not snow.
    ndsi = img.normalizedDifference(['B3', 'B11'])
    score=score.min(rescale(ndsi, 'img', [0.8, 0.6]))

    # Clip the lower end of the score
    score = score.max(ee.Image(0.001))

    # Remove small regions and clip the upper bound
    ## commenting this out--pointless to do this unless we do the
    ## following line (42), which is also commented out in the JS
    ## dilated = dilatedErossion(score).min(ee.Image(1.0))

    ## This was commented out in the javascript--why?
    ## score = score.multiply(dilated)

    score = score.reduceNeighborhood(
        reducer=ee.Reducer.mean(),
        kernel=ee.Kernel.square(5)
      )
    
    print("Generated cloud score")

    return img.addBands(score.rename('cloudScore'))
