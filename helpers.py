import ee

def exportImageCollectionToGCS(imgC, bucket=None, resolution=10, start=False):
    task_ids = {}
    N = int(imgC.size().getInfo())

    for i in range(0, N):
        img = ee.Image(imgC.toList(imgC.size()).get(i))

        filename = str(img.get('FILENAME').getInfo())
        filePath = str(img.get('FILEPATH').getInfo())
        roi = ee.Geometry(img.get("ROI"))
        roi = roi.coordinates().getInfo()

        export = exportImageToGCS(img=img, roi=roi, bucket=bucket, resolution=resolution, filename=filename, dest_path=filePath, start=start)
        task_ids[filename] = export.id

    return(task_ids)

def exportImageToGCS(img=None, roi=None, bucket=None, filename=None, dest_path=None, resolution=10, start=True, sensor_name=None):
    ## same as in the JS version

    if sensor_name == 'copernicus/s2':
        img = img.select(['B4', 'B3', 'B2'])
    elif sensor_name == 'copernicus/s2_sr':
        img = img.select(['TCI_R', 'TCI_G', 'TCI_B'])
        
#     print(img.getInfo())

    export = ee.batch.Export.image.toCloudStorage(
      image=img,
      description=filename,
      scale=resolution,
      region=roi,
      fileNamePrefix=dest_path,
      bucket=bucket,
      maxPixels=1e13
    )
    
#     print()

    if start:
        export.start()
        
         

#     print("exporting final image")
    return export


def exportImageToGDrive(img=None, roi=None, drive_folder=None, filename=None, dest_path=None, resolution=10, start=True):

#downConfig = {
    #'scale': 10, 
    #"maxPixels": 1.0E13, 
    #'driveFolder': 'image3',
    #"driveFileNamePrefix":str(i)
#}  # scale means resolution.
    #image_to_dl = ee.Image(image_dl_list.get(i))
    # img_2 = image_to_dl.select('B.+')
    #img_2 = image_to_dl.select(RGB)
    #name = img_2.getInfo()["id"].split("/")[-1]
    #print("Image to Download:",name)
    #task = ee.batch.Export.image(img_2, name, downConfig)
    #task.start()

    downConfig = {
        'scale': resolution,
        'region': roi,
        'driveFileNamePrefix': dest_path,
        'driveFolder': drive_folder,
        'maxPixels': 1e13,
    }

    export = ee.batch.Export.image(img, filename, downConfig)

    if start:
        export.start()

    return(export)

def rescale(img, exp, thresholds):
    ## identical to the javascript
    return img.expression(exp, {"img": img}) \
              .subtract(thresholds[0]) \
              .divide(thresholds[1] - thresholds[0])

def dilatedErossion(score, dilationPixels=3, erodePixels=1.5):
  # Perform opening on the cloud scores
  ## same as the JS version
  score = score \
            .reproject('EPSG:4326', None, 20) \
            .focal_min(radius=erodePixels, kernelType='circle', iterations=3) \
            .focal_max(radius=dilationPixels, kernelType='circle', iterations=3) \
            .reproject('EPSG:4326', None, 20)

  return(score)

# mergeCollection: Generates a single non-cloudy Sentinel 2 image from a processed ImageCollection
#        input: imgC - Image collection including "cloudScore" band for each image
#              threshBest - A threshold percentage to select the best image. This image is used directly as "cloudFree" if one exists.
#        output: A single cloud free mosaic for the region of interest

def mergeCollection(imgC, keepThresh=5, filterBy='CLOUDY_PERCENTAGE', filterType='less_than', mosaicBy='cloudShadowScore'):
    # Select the best images, which are below the cloud free threshold, sort them in reverse order (worst on top) for mosaicing
    ## same as the JS version
    best = imgC.filterMetadata(filterBy, filterType, keepThresh).sort(filterBy, False)
    #print('Info on first image of collection:', imgC.first().getInfo())
    filtered = imgC.qualityMosaic(mosaicBy)

    # Add the quality mosaic to fill in any missing areas of the ROI which aren't covered by good images
    newC = ee.ImageCollection.fromImages( [filtered, best.mosaic()] )
    
    print("collection merged")

    return ee.Image(newC.mosaic())

# calcCloudCoverage: Calculates a mask for clouds in the image.
#        input: im - Image from image collection with a valid mask layer
#        output: original image with added stats.
#                - CLOUDY_PERCENTAGE: The percentage of the image area affected by clouds
#                - ROI_COVERAGE_PERCENT: The percentage of the ROI region this particular image covers
#                - CLOUDY_PERCENTAGE_ROI: The percentage of the original ROI which is affected by the clouds in this image
#                - cloudScore: A per pixel score of cloudiness
def calcCloudCoverage(img, cloudThresh=0.2):
    imgPoly = ee.Algorithms.GeometryConstructors.Polygon(
              ee.Geometry( img.get('system:footprint') ).coordinates()
              )

    roi = ee.Geometry(img.get('ROI'))
    #line below to used debug issue with export tile pipeline
#     roi = img.geometry()

    intersection = roi.intersection(imgPoly, ee.ErrorMargin(0.5))
    cloudMask = img.select(['cloudScore']).gt(cloudThresh).clip(roi).rename('cloudMask')

    cloudAreaImg = cloudMask.multiply(ee.Image.pixelArea())

    stats = cloudAreaImg.reduceRegion(
      reducer=ee.Reducer.sum(),
      geometry=roi,
      scale=10,
      maxPixels=1e12,
      ## bottom two not in the javascript version
      bestEffort=True,
      tileScale=16
    )

    ## maxAreaError not in the javascript version, which uses the default
    ## for the .area function calls
    maxAreaError = 10
    cloudPercent = ee.Number(stats.get('cloudMask')).divide(imgPoly.area(maxAreaError)).multiply(100)
    coveragePercent = ee.Number(intersection.area(maxAreaError)).divide(roi.area(maxAreaError)).multiply(100)
    cloudPercentROI = ee.Number(stats.get('cloudMask')).divide(roi.area(maxAreaError)).multiply(100)

    img = img.set('CLOUDY_PERCENTAGE', cloudPercent)
    img = img.set('ROI_COVERAGE_PERCENT', coveragePercent)
    img = img.set('CLOUDY_PERCENTAGE_ROI', cloudPercentROI)
    
    print("calculated cloud coverage values")

    return img

def calcCloudCoverage_java(img, cloudThresh=0.2):
    imgPoly = ee.Algorithms.GeometryConstructors.Polygon(
              ee.Geometry( img.get('system:footprint') ).coordinates()
              )

    roi = ee.Geometry(img.get('ROI'))
    

    intersection = roi.intersection(imgPoly, ee.ErrorMargin(0.5))
    cloudMask = img.select(['cloudScore']).gt(cloudThresh).clip(roi).rename('cloudMask')

    cloudAreaImg = cloudMask.multiply(ee.Image.pixelArea())

    stats = cloudAreaImg.reduceRegion(
      reducer=ee.Reducer.sum(),
      geometry=roi,
      scale=10,
      maxPixels=1e12,
    )

    cloudPercent = ee.Number(stats.get('cloudMask')).divide(imgPoly.area()).multiply(100)
    coveragePercent = ee.Number(intersection.area(maxAreaError)).divide(roi.area()).multiply(100)
    cloudPercentROI = ee.Number(stats.get('cloudMask')).divide(roi.area()).multiply(100)

    img = img.set('CLOUDY_PERCENTAGE', cloudPercent)
    img = img.set('ROI_COVERAGE_PERCENT', coveragePercent)
    img = img.set('CLOUDY_PERCENTAGE_ROI', cloudPercentROI)

    return img

def clipToROI(x, roi):
    return x.clip(roi).set('ROI', roi)

def inject_B10(img):
    ee_img_ind = img.get('system:index')
    coll = ee.ImageCollection('COPERNICUS/S2')\
        .filterMetadata('system:index', 'equals', ee_img_ind)
        
    L1C_img = coll.first()
    
    B10 = L1C_img.select('B10')
    
    return img.addBands(B10)

## new function found in the javascript
def computeQualityScore(img):
    score = img.select(['cloudScore']).max(img.select(['shadowScore']))

    score = score.reproject('EPSG:4326', None, 20).reduceNeighborhood(
        reducer=ee.Reducer.mean(),
        kernel=ee.Kernel.square(5)
    )

    score = score.multiply(-1)
    
    print("computed quality score")

    return img.addBands(score.rename('cloudShadowScore'))

## new function found in the javascript
def uniqueValues(collection, field):
    values = ee.Dictionary(
        collection.reduceColumns(ee.Reducer.frequencyHistogram(), [field])\
            .get('histogram')
    ).keys()

    return values
