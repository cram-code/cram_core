;;;
;;; Copyright (c) 2015, Mihai Pomarlan <mpomarlan@yahoo.co.uk>,
;;; All rights reserved.
;;; 
;;; Redistribution and use in source and binary forms, with or without
;;; modification, are permitted provided that the following conditions are met:
;;; 
;;;     * Redistributions of source code must retain the above copyright
;;;       notice, this list of conditions and the following disclaimer.
;;;     * Redistributions in binary form must reproduce the above copyright
;;;       notice, this list of conditions and the following disclaimer in the
;;;       documentation and/or other materials provided with the distribution.
;;;     * Neither the name of Willow Garage, Inc. nor the names of its
;;;       contributors may be used to endorse or promote products derived from
;;;       this software without specific prior written permission.
;;; 
;;; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;;; AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;;; IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
;;; ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
;;; LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
;;; CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
;;; SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
;;; INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
;;; CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
;;; ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
;;; POSSIBILITY OF SUCH DAMAGE.
;;;

(in-package :cpl-impl)

(defclass latch-fluent (value-fluent)
           ((monitored-fluent :initarg :monitored-fluent
                              :initform nil 
                              :reader monitored-fluent
                              :writer (setf monitored-fluent)
                              :documentation "Boolean fluent. When it becomes true, the latch fluent is set to true.")))

(defun setup-latch-fluent (monitored-fluent)
  "Creates a latch fluent over a monitored fluent.
   When monitored-fluent becomes non-NIL, the latch fluent will become T.
   The latch fluent must be manually reset to NIL afterwards."
  (let* ((lf (cpl:make-fluent :name :latch-fluent :class 'latch-fluent :value nil))
         (sf (cpl:fl-funcall (lambda (sig) 
                               (if (cpl:value sig)
                                   (setf (cpl:value lf) T))
                               (cpl:value sig))
                             monitored-fluent)))
    (setf (monitored-fluent lf) sf)
    lf))

(defun setup-accumulator-fluent (monitored-fluent accumulator-function &optional (init-value nil))
  "Creates an accumulator fluent over a monitored fluent. Accumulation is done by accumulator-function,
which must be of the form

(lambda (monitored-value accumulated-value) &body)

and returning a value of type compatible with accumulated-value.

init-value is by default NIL. It is strongly recommended to provide an explicit starting value however,
because not all types of accumulated values are compatible with NIL (for example, REAL isn't).

EXAMPLE: setting up a MAX fluent, which stores the maximum value reached by some other fluent.

(defun acc-max (new-val old-val)
  (if (< old-val new-val)
    new-val
    old-val))

(setup-accumulator-fluent monitored-fluent #'acc-max 0)

NOTE: when setting an accumulator fluent, make sure the monitored-fluent has a reasonable value.

For some fluents it is ok if this initial value is NIL (therefore, we can't use wait-for here).
In some cases however, for example when a monitored-fluent should contain a number, then having
a NIL value while setting up the accumulator will cause an error."
  (let* ((lf (cpl:make-fluent :name :accumulator-fluent :class 'latch-fluent :value init-value))
         (sf (cpl:fl-funcall (lambda (sig)
                               (setf (cpl:value lf) (funcall accumulator-function (cpl:value sig) (cpl:value lf))))
                             monitored-fluent)))
  (setf (monitored-fluent lf) sf)
  lf))

