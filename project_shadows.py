import ee

from helpers import dilatedErossion

# Implementation of Basic cloud shadow shift
# Author: Gennadii Donchyts
# License: Apache 2.0
# Modified by Lloyd Hughes to reduce spurious cloud shadow masks
def sentinel2ProjectShadows(image, cloudHeights=list(range(200, 10000, 250)), cloudThresh=0.2, irSumThresh=0.3, ndviThresh=-0.1):
  meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
  meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

  cloudHeights = ee.List(cloudHeights)

  cloudMask = image.select(['cloudScore']).gt(cloudThresh)

  #Find dark pixels
  darkPixelsImg = image.select(['B8','B11','B12']) \
                    .divide(10000) \
                    .reduce(ee.Reducer.sum())

  ndvi = image.normalizedDifference(['B8','B4'])
  waterMask = ndvi.lt(ndviThresh)

  darkPixels = darkPixelsImg.lt(irSumThresh)

  # Get the mask of pixels which might be shadows excluding water
  darkPixelMask = darkPixels.And(waterMask.Not())
  darkPixelMask = darkPixelMask.And(cloudMask.Not())

  #Find where cloud shadows should be based on solar geometry
  #Convert to radians
  azR = ee.Number(meanAzimuth).add(180).multiply(math.pi).divide(180.0)
  zenR = ee.Number(meanZenith).multiply(math.pi).divide(180.0)

  def findShadows(cloudHeight):
    cloudHeight = ee.Number(cloudHeight)

    shadowCastedDistance = zenR.tan().multiply(cloudHeight)#Distance shadow is cast
    x = azR.sin().multiply(shadowCastedDistance).multiply(-1)#.divide(nominalScale)#X distance of shadow
    y = azR.cos().multiply(shadowCastedDistance).multiply(-1)#Y distance of shadow
    #return cloudMask.changeProj(cloudMask.projection(), cloudMask.projection().translate(x, y))
    return image.select(['cloudScore']).displace(ee.Image.constant(x).addBands(ee.Image.constant(y)))

  #Find the shadows
  shadows = cloudHeights.map( findShadows )

  shadowMasks = ee.ImageCollection.fromImages(shadows)
  shadowMask = shadowMasks.mean()

  # #Create shadow mask
  shadowMask = dilatedErossion(shadowMask.multiply(darkPixelMask))

  shadowScore = shadowMask.reduceNeighborhood(
        reducer=ee.Reducer.max(),
        kernel=ee.Kernel.square(1)
      )

  image = image.addBands(shadowScore.rename(['shadowScore']))

  return image
