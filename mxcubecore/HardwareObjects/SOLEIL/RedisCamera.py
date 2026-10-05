#
#  Project: MXCuBE
#  https://github.com/mxcube
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

import time
import gevent
import logging
import numpy as np
import cv2
import traceback
import redis
import skimage
from typing import Tuple

try:
    import simplejpeg
except ImportError:
    import complexjpeg as simplejpeg

from mxcubecore.utils.qt_import import QImage, QPixmap
from abstract.AbstractVideoDevice import AbstractVideoDevice

def resize_with_pad(image: np.array,
                    new_shape: Tuple[int, int],
                    padding_color: Tuple[int] = (255, 255, 255)) -> np.array:

    #https://gist.github.com/IdeaKing/11cf5e146d23c5bb219ba3508cca89ec
    """Maintains aspect ratio and resizes with padding.
    Params:
        image: Image to be resized.
        new_shape: Expected (width, height) of new image.
        padding_color: Tuple in BGR of padding color
    Returns:
        image: Resized image with padding
    """
    original_shape = (image.shape[1], image.shape[0])
    ratio = float(max(new_shape))/max(original_shape)
    new_size = tuple([int(x*ratio) for x in original_shape])
    image = cv2.resize(image, new_size)
    delta_w = new_shape[0] - new_size[0]
    delta_h = new_shape[1] - new_size[1]
    top, bottom = delta_h//2, delta_h-(delta_h//2)
    left, right = delta_w//2, delta_w-(delta_w//2)
    image = cv2.copyMakeBorder(image, top, bottom, left, right, cv2.BORDER_CONSTANT, value=padding_color)
    return image

class RedisCamera(AbstractVideoDevice):

    def __init__(self, name):
        super(RedisCamera, self).__init__(name)

        self.camera = None
        self.camera_id = str
        self.qimage = None
        self.Contrast = None
        self.Gamma = None
        self.Brightness = None
        self.last_image = None

    def init(self):

        self.redis_local = redis.StrictRedis(host="localhost", protocol=2)

        self.host = self.get_property('host', '172.19.10.181')
        self.port = self.get_property('port', 6379)
        self.protocol = self.get_property('protocol', 3)
        logging.info('init camera connecting to %s %d %d' % (self.host, self.port, self.protocol))
        self.redis = redis.StrictRedis(host=self.host, port=self.port, protocol=self.protocol)

        self.poll_interval = self.get_property('poll_interval', 50)
        self.last_image_data_key = self.get_property('last_image_data_key', 'bzoom:RAW')
        self.last_image_id_key = self.get_property('last_image_id_key', 'last_image_id')
        self.gain_key = self.get_property('gain_key', 'camera_gain')
        self.exposure_time_key = self.get_property('exposure_time_key', 'camera_exposure_time_key')
        self.log = logging.getLogger('HWR')

        self.raw_image_dimensions = [1360, 1024] #[1216, 1024] #[1360, 1024]
        self.scale = 1


        self.image_dimensions = self.get_image_dimensions()
        self.dshape = np.array(self.image_dimensions)

        self.qimage = QImage(np.zeros((int(self.image_dimensions[1]/self.scale), int(self.image_dimensions[0]/self.scale), 3)).data, int(self.image_dimensions[0]/self.scale), 3, QImage.Format_RGB888)

        # Start polling greenlet
        if self.image_polling is None:
            self.set_video_live(True)
            self.change_owner()

            logging.getLogger("HWR").info("Starting polling for camera")
            self.image_polling = gevent.spawn(
                self.do_image_polling, self.poll_interval / 1000.0
            )
            self.image_polling.link_exception(self.polling_ended_exc)
            self.image_polling.link(self.polling_ended)

        self.set_is_ready(True)

    def get_raw_image_size(self):
        return self.raw_image_dimensions

    def do_image_polling(self, sleep_time=0.01):

        last_image_id = None

        while True:
            #new_image_id = self.redis.get(self.last_image_id_key)
            #if True: #last_image_id != new_image_id:
            try:
                img = self.get_last_image()
                if img is not None:
                    self.qimage = QImage(
                        img,
                        img.shape[0],
                        img.shape[1],
                        QImage.Format_RGB888)
                    self.emit("imageReceived", QPixmap(self.qimage))
                    #last_image_id = new_image_id
            except:
                logging.getLogger("HWR").error(traceback.format_exc())
            gevent.sleep(sleep_time)

    def shape_differs(self, shape):
        return shape[0] != self.image_dimensions[0] or shape[1] != self.image_dimensions[1]

    def get_last_image(self):
        img = None
        mxcube_camera = self.redis_local.get("mxcube_camera").decode()
        #if mxcube_camera in ["default", "oav"]:
            #last_image_data = self.redis.get(self.last_image_data_key)
            ##logging.info('get_last_image %s' % simplejpeg.is_jpeg(last_image_data))
        #else:
            #last_image_data = self.redis_local.get(f"value_{mxcube_camera}")

        if mxcube_camera in ["default", "oav"]:
            mxcube_camera = "oav_camera"
        last_image_data = self.redis_local.get(f"value_{mxcube_camera}")

        if last_image_data is not None and simplejpeg.is_jpeg(last_image_data):
            try:
                img = simplejpeg.decode_jpeg(last_image_data)
                if self.shape_differs(img.shape):
                    img = resize_with_pad(img, self.image_dimensions)
                    #d = (self.dshape / shape).max()
                    #v, h = (shape / d).astype(int)
                    #img = cv.resize(img, (h, v))
                img = img.reshape((self.image_dimensions[0], self.image_dimensions[1], 3))
            except ValueError:
                img = self.last_image
            #logging.info('get_last_image %s shape %s dtype %s' % (simplejpeg.is_jpeg(last_image_data), img.shape, img.dtype))
        elif last_image_data is not None:
            header_size = 24
            img = np.ndarray(buffer=last_image_data[header_size:], dtype=np.uint8, shape=(self.image_dimensions[0], self.image_dimensions[1], 3))

        if img is not None:
            self.last_image = img
        if self.scale != 1:
            try:
                img = (skimage.transform.rescale(img, 1/self.scale, anti_aliasing=True, multichannel=True, mode='reflect')*255).astype('uint8')
            except:
                img = np.zeros((int(self.image_dimensions[0]/self.scale), int(self.image_dimensions[1]/self.scale), 3))
        return img

    def get_new_image(self):
        return self.qimage

    def get_video_live(self):
        return True

    def start_camera(self):
        pass

    def change_owner(self):
        pass

    def get_contrast(self):
        try:
            return self.Contrast
        except:
            self.log.exception(traceback.format_exc())

    def set_contrast(self, contrast_value):
        try:
            self.Contrast = contrast_value
        except:
            self.log.exception(traceback.format_exc())

    def get_brightness(self):
        try:
            return self.Brightness
        except:
            self.log.exception(traceback.format_exc())

    def set_brightness(self, brightness_value):
        try:
            self.Brightness = brightness_value
        except:
            self.log.exception(traceback.format_exc())

    def get_gain(self):
        gain = float(self.redis.get(self.gain_key))
        return gain

    def set_gain(self, gain_value):
        try:
            if gain_value != None:
                self.redis.set(self.gain_key, gain_value)
        except:
            self.log.exception(traceback.format_exc())

    def get_gamma(self):
        try:
            return self.Gamma
        except:
            self.log.exception(traceback.format_exc())

    def set_gamma(self, gamma_value):
        try:
            self.Gamma = gamma_value
        except:
            self.log.exception(traceback.format_exc())

    def get_exposure_time(self):
        try:
            exposure_time = float(self.redis.get(self.exposure_time_key))
            return exposure_time
            #if self.camera != None:
                #return self.camera.ExposureTimeAbs/1.e6
        except:
            self.log.exception(traceback.format_exc())

    def set_exposure_time(self, exposure_time_value):
        # time unit used by API is microsecond
        # while we typically operate in seconds
        self.log.info('exposure_time_value %.3f' % exposure_time_value)
        exposure_time_value = (exposure_time_value < 5  and exposure_time_value > 0.001) and exposure_time_value or exposure_time_value/1.e3
        self.log.info('after adjustment exposure_time_value %.3f' % exposure_time_value)
        try:
            self.redis.set(self.exposure_time_key, exposure_time_value)
            #if self.camera != None:
                #self.camera.ExposureTimeAbs = int(exposure_time_value * 1.e6)
        except:
            self.log.exception(traceback.format_exc())
