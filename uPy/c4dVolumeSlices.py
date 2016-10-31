# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 2016

@author: DMT
"""

import c4d
import os
import sys
import upy
from upy import uiadaptor

upy.setUIClass()

# get the helperClass for modeling
helperClass = upy.getHelperClass()


class PlaneSliceMaker(uiadaptor):
    def setup(self, vi=None):
        # get the helper
        self.helper = helperClass(vi=vi)
        # dont want to dock it ie maya
        self.dock = False
        # initialize widget and layout
        self.initWidget()
        self.setupLayout()

    # these two functions are for c4d
    def CreateLayout(self):
        self._createLayout()
        return 1

    def Command(self, *args):
        # print args
        self._command(args)
        return 1

    def loadPlanes(self, dirname, objprefix, scale=0.1):
        self.myimages = []  # list of image filenames
        dirFiles = os.listdir(dirname)  # list of directory files
        for ifile in dirFiles:  # filter out all non images
            if '.jpg' in ifile:
                self.myimages.append(ifile)
            elif '.png' in ifile:
                self.myimages.append(ifile)
            elif '.tif' in ifile:
                self.myimages.append(ifile)
            elif '.tiff' in ifile:
                self.myimages.append(ifile)
        print dirFiles
        self.myimages.sort(key=lambda f: int(filter(str.isdigit, f) or -1))
        self.dirname = dirname

    def createSlices(self):
        nr = self.helper.newEmpty('VOLUME', display=1, visible=1)
        m = c4d.Matrix()
        nr.SetMg(m)
        for i in range(len(self.myimages)):
            mat = self.helper.addMaterial("MAT_" + str(i), color=[1, 1, 1])

            mat[c4d.MATERIAL_USE_COLOR] = True
            colorbmp = c4d.BaseList2D(c4d.Xbitmap)
            colorbmp[c4d.BITMAPSHADER_FILENAME] = os.path.join(self.dirname, self.myimages[i])
            mat[c4d.MATERIAL_COLOR_SHADER] = colorbmp
            mat.InsertShader(colorbmp)

            mat[c4d.MATERIAL_USE_ALPHA] = True

            alphabmp = c4d.BaseList2D(c4d.Xbitmap)
            alphabmp[c4d.BITMAPSHADER_FILENAME] = os.path.join(self.dirname, self.myimages[i])
            mat.InsertShader(alphabmp)

            mask = c4d.BaseList2D(c4d.Xcolor)
            maskdata = mask.GetDataInstance()
            # mask.Message(c4d.MSG_UPDATE)
            maskdata.SetData(c4d.COLORSHADER_COLOR, c4d.Vector(0.13, 0.13, 0.13))
            mat.InsertShader(mask)

            blend = c4d.BaseList2D(c4d.Xcolor)
            blenddata = blend.GetDataInstance()
            # mask.Message(c4d.MSG_UPDATE)
            blenddata.SetData(c4d.COLORSHADER_COLOR, c4d.Vector(1, 1, 1))
            mat.InsertShader(blend)

            data = mat.GetDataInstance()
            col = c4d.BaseList2D(c4d.Xfusion)
            coldata = col.GetDataInstance()
            # Set here your texture path, relative or absolute doesn't matter
            coldata.SetData(c4d.SLA_FUSION_USE_MASK, 1)
            coldata.SetLink(c4d.SLA_FUSION_BLEND_CHANNEL, alphabmp)
            coldata.SetLink(c4d.SLA_FUSION_MASK_CHANNEL, mask)
            coldata.SetLink(c4d.SLA_FUSION_BASE_CHANNEL, blend)
            coldata.SetData(c4d.SLA_FUSION_INVERT_OUTPUT, 1)
            # col.Message(c4d.MSG_UPDATE)
            data.SetLink(c4d.MATERIAL_ALPHA_SHADER, col)
            mat.InsertShader(col)

            mat[c4d.MATERIAL_USE_TRANSPARENCY] = False
            mat[c4d.MATERIAL_USE_SPECULAR] = False
            mat[c4d.MATERIAL_USE_REFLECTION] = False

            sx = self.getVal(self.sizeInputX)
            sy = self.getVal(self.sizeInputY)
            sz = self.getVal(self.sizeInputZ)
            pl = self.helper.plane("PLANE"+str(i), center=[0., i*sz/(len(self.myimages)-1.0), 0.],
                                   size=[sx, sy], axis='+Y', parent=nr)
            self.helper.assignMaterial(pl[0], mat, texture=True)
            # print("CREATED PLANE " + str(i))

    def loadGeneral(self, filename):
        self.loadPlanes(filename, "Test", 4.0)

    def browseSlices(self, *args):
        # first need to call the ui fileDialog
        # self.fileDialog(label="choose a file", callback=self.loadGeneral,
        #                 suffix="jpg")
        # need a DIRECTORY picker...
        filename = c4d.storage.LoadDialog(c4d.FSTYPE_ANYTHING,
                                          title="choose a directory",
                                          flags=c4d.FILESELECT_DIRECTORY)
        print(filename)
        self.setVal(self.pathLabel, filename)
        return self.loadGeneral(filename)

    def initWidget(self, id=None):
        # this is where we define the buttons
        self.PUSHS = {}
        self.browseButton = self._addElemt(name="Browse Dir",
                                           width=40,
                                           height=10,
                                           action=self.browseSlices,
                                           type="button")
        self.pathLabel = self._addElemt(name="Path",
                                        width=400,
                                        height=10,
                                        type="label",
                                        label='./')
        self.sizeLabelX = self._addElemt(name="SizeX",
                                         width=40,
                                         height=10,
                                         type="label",
                                         label='Size(X)')
        self.sizeInputX = self._addElemt(name="SizeX",
                                         width=40,
                                         height=10,
                                         value=100,
                                         maxi=100000,
                                         type='inputFloat')
        self.sizeLabelY = self._addElemt(name="SizeY",
                                         width=40,
                                         height=10,
                                         type="label",
                                         label='Size(Y)')
        self.sizeInputY = self._addElemt(name="SizeY",
                                         width=40,
                                         height=10,
                                         value=100,
                                         maxi=100000,
                                         type='inputFloat')
        self.sizeLabelZ = self._addElemt(name="SizeZ",
                                         width=40,
                                         height=10,
                                         type="label",
                                         label='Size(Z)')
        self.sizeInputZ = self._addElemt(name="SizeZ",
                                         width=40,
                                         height=10,
                                         value=100,
                                         maxi=100000,
                                         type='inputFloat')
        self.generateButton = self._addElemt(name="Generate",
                                             width=40,
                                             height=10,
                                             action=self.createSlices,
                                             type="button")

    def setupLayout(self):
        # this where we define the Layout
        # this wil make three button on a row
        # self._layout is a class variable that you need to use
        # you may use your own, but remember to reassign to self._layout at the end.
        # for instance:
        # mylayout=[]
        # mylayout.append([widget])
        # self._layout=mylayout
        self._layout = []

        self._layout.append([self.browseButton, self.pathLabel])
        self._layout.append([self.sizeLabelX, self.sizeInputX])
        self._layout.append([self.sizeLabelY, self.sizeInputY])
        self._layout.append([self.sizeLabelZ, self.sizeInputZ])
        self._layout.append([self.generateButton])

# this is a script, we need some special code for the Tk and the Qt case.
# the most important part are the instanciation of our dialog class,
# and the two functio setup and display
# setup initialise the widget and the layout
# display wil actually show the dialog.
if uiadaptor.host == "tk":
    from DejaVu import Viewer
    vi = Viewer()
    # require a master
    # import Tkinter #Tkinter if python2.x tkinter for python3.x
    # root = Tkinter.Tk()
    mygui = PlaneSliceMaker(title="Plane Slice Maker", master=vi)
    mygui.setup(vi=vi)
    # mygui.display()
elif uiadaptor.host == "qt":
    from PyQt4 import QtGui
    app = QtGui.QApplication(sys.argv)
    mygui = PlaneSliceMaker(title="Plane Slice Maker")
    mygui.setup()
    # ex.show()
else:
    mygui = PlaneSliceMaker(title="Plane Slice Maker")
    mygui.setup()
    # call it
mygui.display()
if uiadaptor.host == "qt":
    app.exec_()  # work without it ?
