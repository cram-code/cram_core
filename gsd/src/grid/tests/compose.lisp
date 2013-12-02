;; Tests of grid composition functions
;; Liam Healy 2009-11-28 17:12:53EST compose.lisp
;; Time-stamp: <2010-07-15 09:41:17EDT compose.lisp>
;;
;; Copyright 2009 Liam M. Healy
;; Distributed under the terms of the GNU General Public License
;;
;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

(in-package :grid)

;;;;****************************************************************************
;;;; Drop
;;;;****************************************************************************

(lisp-unit:define-test drop
  (lisp-unit:assert-numerical-equal
   '(0.0d0 10.0d0 20.0d0 30.0d0 40.0d0)
   (contents (drop (test-grid-double-float 'array '(5 1)))))
  (lisp-unit:assert-numerical-equal
   '(0.0d0 1.0d0 2.0d0 3.0d0 4.0d0)
   (contents (drop (test-grid-double-float 'array '(1 5))))))

;;;;****************************************************************************
;;;; Subgrid
;;;;****************************************************************************

(lisp-unit:define-test subgrid
  (lisp-unit:assert-numerical-equal
   '(0.0d0 1.0d0 2.0d0 3.0d0)
   (contents (row (test-grid-double-float 'array '(3 4)) 0)))
  (lisp-unit:assert-numerical-equal
   '(10.0d0 11.0d0 12.0d0 13.0d0)
   (contents (row (test-grid-double-float 'array '(3 4)) 1)))
  (lisp-unit:assert-numerical-equal
   '(20.0d0 21.0d0 22.0d0 23.0d0)
   (contents (row (test-grid-double-float 'array '(3 4)) 2)))
  (lisp-unit:assert-numerical-equal
   '(0.0d0 10.0d0 20.0d0)
   (contents (column (test-grid-double-float 'array '(3 4)) 0)))
  (lisp-unit:assert-numerical-equal
   '(1.0d0 11.0d0 21.0d0)
   (contents (column (test-grid-double-float 'array '(3 4)) 1)))
  (lisp-unit:assert-numerical-equal
   '(2.0d0 12.0d0 22.0d0)
   (contents (column (test-grid-double-float 'array '(3 4)) 2)))
  (lisp-unit:assert-numerical-equal
   '(3.0d0 13.0d0 23.0d0)
   (contents (column (test-grid-double-float 'array '(3 4)) 3)))
  (lisp-unit:assert-numerical-equal
   '((12.0d0 13.0d0) (22.0d0 23.0d0))
   (contents (subgrid (test-grid-double-float 'array '(3 4)) '(2 2) '(1 2)))))

(lisp-unit:define-test subgrid-setf 
  (lisp-unit:assert-numerical-equal
   ;; Inject a rank 1 subgrid vertically in a rank 2 grid
   '((0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (let ((dest (make-grid '((array 5 5) double-float) :initial-element 0.0d0))
	 (src (make-grid '((array 3) double-float) :initial-element 77.0d0)))
     (setf (subgrid dest '(1 2)) src)
     (contents dest)))
  (lisp-unit:assert-numerical-equal
   ;; Inject a rank 1 subgrid horizontally in a rank 2 grid
   '((0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 77.0d0 77.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (let ((dest (make-grid '((array 5 5) double-float) :initial-element 0.0d0))
	 (src (make-grid '((array 3) double-float) :initial-element 77.0d0)))
     (setf (subgrid dest '(1 2) '(1)) src)
     (contents dest)))
  (lisp-unit:assert-numerical-equal
   ;; Inject a rank 2 subgrid in a rank 2 grid
   '((0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 77.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 77.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 77.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (let ((dest (make-grid '((array 5 5) double-float) :initial-element 0.0d0))
	 (src (make-grid '((array 3 2) double-float) :initial-element 77.0d0)))
     (setf (subgrid dest '(1 2)) src)
     (contents dest)))
  (lisp-unit:assert-numerical-equal
   ;; Inject a rank 2 subgrid transposed in a rank 2 grid
   '((0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 77.0d0 77.0d0 77.0d0)
     (0.0d0 0.0d0 77.0d0 77.0d0 77.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (let ((dest (make-grid '((array 5 5) double-float) :initial-element 0.0d0))
	 (src (make-grid '((array 3 2) double-float) :initial-element 77.0d0)))
     (setf (subgrid dest '(1 2) '(1 0)) src)
     (contents dest)))
  (lisp-unit:assert-numerical-equal
   ;; Set a row
   '((0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (77.0d0 77.0d0 77.0d0 77.0d0 77.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (let ((dest (make-grid '((array 5 5) double-float) :initial-element 0.0d0))
	 (src (make-grid '((array 5) double-float) :initial-element 77.0d0)))
     (setf (row dest 2) src)
     (contents dest))
   (lisp-unit:assert-numerical-equal
    ;; Set a column
    '((0.0d0 0.0d0 77.0d0 0.0d0 0.0d0)
      (0.0d0 0.0d0 77.0d0 0.0d0 0.0d0)
      (0.0d0 0.0d0 77.0d0 0.0d0 0.0d0)
      (0.0d0 0.0d0 77.0d0 0.0d0 0.0d0)
      (0.0d0 0.0d0 77.0d0 0.0d0 0.0d0))
    (let ((dest (make-grid '((array 5 5) double-float) :initial-element 0.0d0))
	  (src (make-grid '((array 5) double-float) :initial-element 77.0d0)))
      (setf (column dest 2) src)
      (contents dest)))))

;;;;****************************************************************************
;;;; Transpose
;;;;****************************************************************************

(lisp-unit:define-test transpose
  (lisp-unit:assert-numerical-equal
   '((0.0d0 10.0d0 20.0d0) (1.0d0 11.0d0 21.0d0)
     (2.0d0 12.0d0 22.0d0) (3.0d0 13.0d0 23.0d0))
   (contents (transpose (test-grid-double-float 'array '(3 4)))))
  (lisp-unit:assert-numerical-equal
   '((0.0d0 10.0d0 20.0d0 30.0d0 40.0d0))
   (contents (transpose  (test-grid-double-float 'array '(5 1)))))
  (lisp-unit:assert-numerical-equal
   '((0.0d0 10.0d0 20.0d0 0.0d0 0.0d0)
     (1.0d0 11.0d0 21.0d0 0.0d0 0.0d0)
     (2.0d0 12.0d0 22.0d0 0.0d0 0.0d0)
     (3.0d0 13.0d0 23.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (contents (transpose
	      (test-grid-double-float 'array '(3 4))
	      :destination
	      (make-array '(5 5) :element-type 'double-float :initial-element 0.0d0))))
  (lisp-unit:assert-numerical-equal
   '((0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 10.0d0 20.0d0 0.0d0)
     (0.0d0 1.0d0 11.0d0 21.0d0 0.0d0)
     (0.0d0 2.0d0 12.0d0 22.0d0 0.0d0)
     (0.0d0 3.0d0 13.0d0 23.0d0 0.0d0))
   (contents (transpose
	      (test-grid-double-float 'array '(3 4))
	      :destination
	      (make-array '(5 5) :element-type 'double-float :initial-element 0.0d0)
	      :start '(1 1))))
  #+(or)			   ; not yet ready for composite grids
  (lisp-unit:assert-numerical-equal
   (MAKE-GRID '((LIST 5) (LIST 5) DOUBLE-FLOAT) :INITIAL-CONTENTS
	      '(0.0d0 10.0d0 20.0d0 0.0d0 0.0d0 1.0d0 11.0d0 21.0d0 0.0d0
		0.0d0 2.0d0 12.0d0 22.0d0 0.0d0 0.0d0 3.0d0 13.0d0 23.0d0
		0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (transpose
    (test-grid-double-float 'array '(3 4))
    :destination
    (make-grid '((list 5) (list 5) double-float))))
  #+(or)			   ; not yet ready for composite grids
  (lisp-unit:assert-numerical-equal
   (MAKE-GRID '((LIST 5) (ARRAY 5) DOUBLE-FLOAT) :INITIAL-CONTENTS
	      '(0.0d0 10.0d0 20.0d0 0.0d0 0.0d0 1.0d0 11.0d0 21.0d0 0.0d0
		0.0d0 2.0d0 12.0d0 22.0d0 0.0d0 0.0d0 3.0d0 13.0d0 23.0d0
		0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0))
   (transpose
    (test-grid-double-float 'array '(3 4))
    :destination
    (make-grid '((list 5) (array 5) double-float)))))

;;;;****************************************************************************
;;;; Diagonal
;;;;****************************************************************************

(lisp-unit:define-test diagonal
  (lisp-unit:assert-numerical-equal
   '(0.0d0 11.0d0 22.0d0)
   (contents (diagonal (test-grid-double-float 'array '(3 4)))))
  (lisp-unit:assert-numerical-equal
   '(1.0d0 12.0d0 23.0d0)
   (contents (diagonal (test-grid-double-float 'array '(3 4)) :offset 1)))
  (lisp-unit:assert-numerical-equal
   '(2.0d0 13.0d0)
   (contents (diagonal (test-grid-double-float 'array '(3 4)) :offset 2)))
  (lisp-unit:assert-numerical-equal
   '(3.0d0)
   (contents (diagonal (test-grid-double-float 'array '(3 4)) :offset 3)))
  (lisp-unit:assert-numerical-equal
   '(10.0d0 21.0d0)
   (contents (diagonal (test-grid-double-float 'array '(3 4)) :offset -1)))
  (lisp-unit:assert-numerical-equal
   '(20.0d0)
   (contents (diagonal (test-grid-double-float 'array '(3 4)) :offset -2)))
  (lisp-unit:assert-numerical-equal
   '((1.0d0 0.0d0 0.0d0) (0.0d0 2.0d0 0.0d0) (0.0d0 0.0d0 3.0d0))
   (let ((arr3x3 (make-grid '((array 3 3) double-float)
			    :initial-element 0.0d0)))
     (setf (diagonal arr3x3) #(1.0d0 2.0d0 3.0d0))
     (contents arr3x3)))
  (lisp-unit:assert-numerical-equal
   '((1.0d0 0.0d0 0.0d0) (0.0d0 1.0d0 0.0d0) (0.0d0 0.0d0 1.0d0))
   (contents (identity-matrix 3)))
  (lisp-unit:assert-numerical-equal
   '((1.0d0 2.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (-2.0d0 1.0d0 2.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 -2.0d0 1.0d0 2.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 -2.0d0 1.0d0 2.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 -2.0d0 1.0d0 2.0d0 0.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 -2.0d0 1.0d0 2.0d0 0.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 -2.0d0 1.0d0 2.0d0 0.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 -2.0d0 1.0d0 2.0d0 0.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 -2.0d0 1.0d0 2.0d0)
     (0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 0.0d0 -2.0d0 1.0d0))
   (let ((g (make-grid '((array 10 10) double-float) :initial-element 0)))
     (set-diagonal g 1 0 t)
     (set-diagonal g 2 1 t)
     (set-diagonal g -2 -1 t)
     (contents g)))
  ;; Low-level inject
  (lisp-unit:assert-numerical-equal
   '(-77.7d0 -77.7d0 -77.7d0 0.0d0 11.0d0 22.0d0)
   (contents
    (map-grid :source (test-grid-double-float 'array '(3 4))
	      :source-affi (affi:diagonal (affi (test-grid-double-float 'array '(3 4))))
	      :destination-affi (affi:make-affi '(6) 3)
	      :initial-element -77.7d0)))
  (lisp-unit:assert-numerical-equal
   '(-77.7d0 -77.7d0 -77.7d0 -77.7d0 -66.7d0 -55.7d0)
   (contents
    (map-grid :source (test-grid-double-float 'array '(3 4))
	      :source-affi (affi:diagonal (affi (test-grid-double-float 'array '(3 4))))
	      :destination-affi (affi:make-affi '(6) 3)
	      :initial-element -77.7d0
	      :combination-function '+))))

;;;;****************************************************************************
;;;; Concatenate
;;;;****************************************************************************

(lisp-unit:define-test concatenate
  (lisp-unit:assert-numerical-equal
   '((0.0d0 1.0d0 2.0d0 3.0d0)
     (10.0d0 11.0d0 12.0d0 13.0d0)
     (20.0d0 21.0d0 22.0d0 23.0d0)
     (0.5d0 1.5d0 2.5d0 3.5d0)
     (10.5d0 11.5d0 12.5d0 13.5d0))
   (contents
    (concatenate-grids
     (map-grid :source 'index-fill-decadal
	       :destination-specification '((array 3 4) double-float))
     (map-grid :source (offset-ifd 0.5d0)
	       :destination-specification '((array 2 4) double-float)))))
  (lisp-unit:assert-numerical-equal
   '((0.5d0 1.5d0 2.5d0 3.5d0 0.1d0 1.1d0)
     (10.5d0 11.5d0 12.5d0 13.5d0 10.1d0 11.1d0)
     (20.5d0 21.5d0 22.5d0 23.5d0 20.1d0 21.1d0))
   (contents
    (concatenate-grids
     (map-grid :source (offset-ifd 0.5d0)
	       :destination-specification '((array 3 4) double-float))
     (map-grid :source (offset-ifd 0.1d0)
	       :destination-specification '((array 3 2) double-float))
     :axis 1)))
  #+(or)			   ; not yet ready for composite grids
  (lisp-unit:assert-numerical-equal
   (MAKE-GRID '((LIST 3) (LIST 6) DOUBLE-FLOAT) :INITIAL-CONTENTS
	      #(0.5d0 1.5d0 2.5d0 3.5d0 0.1d0 1.1d0 10.5d0 11.5d0 12.5d0
		13.5d0 10.1d0 11.1d0 20.5d0 21.5d0 22.5d0 23.5d0 20.1d0
		21.1d0))
   (concatenate-grids
    (map-grid :source (offset-ifd 0.5d0)
	       :destination-specification '((array 3 4) double-float))
    (map-grid :source (offset-ifd 0.1d0)
	       :destination-specification '((array 3 2) double-float))
    :axis 1 :gtype-OBSOLETE '(list list double-float))))
