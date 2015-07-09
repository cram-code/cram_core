;;;
;;; Copyright (c) 2015, Mihai Pomarlan <mpomarlan@yahoo.co.uk>
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

;;; This file defines function versions of some of the macros that make up the plan language (seq, par, try-in-order etc.)
;;;
;;; The reason is that these ptr functions show up in the task tree. In particular, their ptr-argument is changeable by
;;; plan transformation, which means the plan could decide, at runtime, what to put inside a sequence or parallel block.

;;; TODO: right now all these anonymous lambdas will mess up de/serialization. Will need some way to detect whether
;;; ptr-parameter slots in the task tree are safely serializable and/or provide a serialization mechanism for some
;;; reasonable class of function objects/closures.

(def-ptr-cram-function ptr-seq (ptr-parameter)
"Run function objects from ptr-parameter in sequence. Returns the return value of the last function object.

PTR-PARAMETER must be a list of function objects. Example:

(list (lambda () (some-function arg-1 arg-2...)) (lambda () ...))

Function objects should wrap functions defined with def-[ptr-]cram-function if they are to show up in the task tree.

Will fail as soon as one of the function objects produces a failure."
  (car (last (mapcar #'funcall ptr-parameter))))

(def-ptr-cram-function ptr-try-in-order (ptr-parameter)
  "PTR-PARAMETER must be a list of function objects. Example:

   (list (lambda () (some-function arg-1 arg-2...)) (lambda () ...))

   Execute function objects in ptr-parameter sequentially. Succeed if one succeeds, fail if all fail.

   Return value is the return value of the first function object in ptr-parameter that succeeds.
   In case of failure on all function objects, a composite-failure is signaled."
  (block ablock
    (let* ((failures (list)))
      (mapcar (lambda (form)
                (block tryout-block
                  (with-failure-handling 
                    ((plan-failure (err)
                      (setf failures (cons err failures))
                      (return-from tryout-block)))
                    (return-from ablock (funcall form)))))
              ptr-parameter)
      (assert-no-returning
        (signal
          (make-condition 'composite-failure
                          :failures (reverse failures)))))))

(def-ptr-cram-function ptr-with-task-suspended (ptr-parameter task &key reason)
  "PTR-PARAMETER must be a list of function objects. Example:

   (list (lambda () (some-function arg-1 arg-2...)) (lambda () ...))

   Execute function objects in ptr-parameter with 'task' being suspended.

   Returns the value returned by the last function object in ptr-parameter.
   (NOTE: the return value is a difference to the with-task-suspended macro.)"
  (let* ((task-sym task)
         (retq nil))
    (unwind-protect
      (progn
        (suspend task-sym :sync t :reason reason)
        (wait-for (fl-eq (status task-sym) :suspended))
        (setf retq (car (last (mapcar #'funcall ptr-parameter))))
    (wake-up task-sym)
    retq))))

(def-ptr-cram-function ptr-try-each-in-order (ptr-parameter)
  "PTR-PARAMETER must be a list containing (function-object list)

   The function object must have a lambda list with exactly one
   argument. Example:
   
   (lambda (arg) ...)

   Applies function-object to each element in `list' sequentially until 
   function-object succeeds. Returns the result of function-object as 
   soon as it succeeds and stops iterating. Otherwise, if all attempts 
   fail, signal a composite failure.

   NOTE: there's a difference here to the try-each-in-order macro:
   rather than bind the element of list to some global variable, it
   is passed as a parameter to function-object. If you do want to 
   bind the element to a global variable, you'd need something like:

   (ptr-try-each-in-order
     (list
       (lambda (arg)
         (setf global-variable arg)
         (some-function))
       list-of-options))"
  (block ablock 
    (let* ((failures (list))
           (opt-list (second ptr-parameter))
           (function-object (first ptr-parameter)))
      (dolist (arg opt-list (assert-no-returning
                              (signal
                                (make-condition 'composite-failure
                                                :failures (reverse failures)))))
        (block try-block
          (with-failure-handling 
            ((plan-failure (condition)
               (setf failures (cons condition failures))
               (return-from try-block)))
            (return-from ablock (funcall function-object arg))))))))

;;;; TODO: implement ptr-functions of the following:

;;(def-plan-macro with-task ((&key (class 'task) (name "WITH-TASK")) &body body)
;;  "Executes body in a separate task and joins it." ...)

;;(defmacro with-parallel-childs (name (running done failed) child-forms
;;                                &body watcher-body)
;;  "Execute `child-forms' in parallel and execute `watcher-body'
;;   whenever any child changes its status.
;;
;;   Lexical bindings are established for `running', `done' and `failed'
;;   around `watcher-body', and bound to lists of all running, done and
;;   failed tasks. `watcher-body' is executed within an implicit block
;;   named NIL.
;;
;;   `name' is supposed to be the name of the plan-macro that's
;;   implemented on top of WITH-PARALLEL-CHILDS. `name' will be used to
;;   name the tasks spawned for `child-forms'.
;;
;;   All spawned child tasks will be terminated on leave." ...)

;;(def-plan-macro par (&body forms)
;;  "Executes forms in parallel. Fails if one fails. Succeeds if all
;;   succeed." ...)

;;(def-plan-macro pursue (&body forms)
;;  "Execute forms in parallel. Succeed if one succeeds, fail if one
;;   fails." ...)

;;(def-plan-macro try-all (&body forms)
;;  "Try forms in parallel. Succeed if one succeeds, fail if all fail.
;;   In the case of a failure, a condition of type 'composite-failure'
;;   is signaled, containing the list of all error messages and data." ...)

;;(def-plan-macro partial-order ((&body steps) &body orderings)
;;  "Specify ordering constraints for `steps'. `steps' are executed in
;;an implicit PAR form. `orderings' is a list of orderings. An ordering
;;always has the form:
;;
;;  (:order <contstraining-task> <constrained-task>)
;;
;;`constraining-task' and `constrained-task' are task objects. That
;;means, they can be either be defined in the current lexical
;;environment (over a :tag) or by either using the function TASK to
;;reference the task by its absolute path or the function SUB-TASK to
;;reference it by its path relative to the PARTIAL-ORDER form." ...)

;;(def-plan-macro par-loop ((var sequence) &body body)
;;  "Executes body in parallel for each `var' in `sequence'." ...)
