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

;; TODO: use keyvars here
(defparameter ptr-retry nil)
(defparameter ptr-return nil)
(defparameter ptr-fail nil)

(define-condition fluent-condition-failure (condition)
  ((failed-fluents
    :initarg :failed-fluents
    :initform nil)))

(define-condition goal-pattern-not-found (condition)
  ((pattern
    :initarg :pattern
    :initform nil)
   (goal
    :initarg :goal
    :initform nil)))

(define-condition goal-recipe-not-found (condition)
  ((name
    :initarg :name
    :initform nil)
   (pattern
    :initarg :pattern
    :initform nil)
   (goal
    :initarg :goal
    :initform nil)))

(define-condition goal-recipes-failed (condition)
  ;; DO NOT make this a subtype of goal-pattern-not-found (or vice-versa).
  ;; We do not want handlers for one to inadvertently catch the other.
  ((pattern
    :initarg :pattern
    :initform nil)
   (goal
    :initarg :goal
    :initform nil)))

(defclass failure-monitored-process ()
  ((mutex
     :reader fmp/mutex
     ;; no writer, will never need to put a new mutex in the slot (the mutex's own slots are already changeable anyway)
     ;; no initarg, default init below is the only sensible value
     :initform (make-instance 'sb-thread:mutex)
     :documentation "Needed if we want to change recipes/their scores on the fly.")
   (parent
     :accessor fmp/parent ;; do not export
     :initarg :parent
     :initform nil
     :documentation "Used to trace which recipe belongs to which pattern, which pattern to which goal.")
   (fluent-conditions
     :reader fmp/fluent-conditions
     ;; no writer, will never need to put a new hash in the slot (the hash contents are already changeable anyway)
     ;; no initarg, default init below is the only sensible value
     :initform (make-hash-table :test #'eq)
     :documentation "A hash of (key:fluent expected-value) pairs. If any of the fluents in the hash deviates from its expected value, issue a fluent-condition-failure.")
   (name
     :reader fmp/name
     ;; no writer, will never change a recipe name
     :initarg :name
     :initform nil
     :documentation "Used to identify this particular recipe- each recipe in a pattern should have a unique name.")
   (body
    :accessor fmp/body
    :initarg :body
    :initform nil
    :documentation "Function object of the proc.") ;;; Or, should we use source code here and do eval?
   (failure-handlers
     :accessor fmp/failure-handlers
     :initform (make-hash-table :test #'eq)
     :type hash-table
     :documentation "A hash-table of (failure-type: function) pairs. Function objects should take exactly one parameter of condition type.")))

(defclass ptr-goal-pattern-recipe (failure-monitored-process)
  ((score
     :accessor gpr/score
     :initarg :score
     :initform 0
     :type number
     :documentation "Used to rank this recipe vs. others in a ptr-goal-pattern. Higher score means a recipe is tried first.")))

(defclass ptr-goal-pattern (failure-monitored-process)
  ((pattern
    :reader gp/pattern
    ;; no writer, will never change a pattern
    :initarg :pattern
    :initform nil
    :documentation "Pattern to match, expressed in the usual Prolog-in-Lisp way (? prefixes variable names).")
   (recipes
    :accessor gp/recipes
    :initarg :recipes
    :initform nil
    :type list
    :documentation "List of recipes to pursue a particular goal pattern.")))

(defclass ptr-goal (failure-monitored-process)
  ((function
    :reader goal/function
    :writer (setf goal/function-w)
    :initarg :function
    :initform nil
    :documentation "A link to the main function responsible to carry out this goal.")
   (patterns
    :reader goal/patterns
    :writer (setf goal/patterns-w)
    :initarg :patterns
    :initform nil
    :type list
    :documentation "List of patterns (each with a collection of recipes to carry them out).")))

(defun find-handler (fmp err)
  (sb-thread:with-mutex ((fmp/mutex fmp))
    (loop for ftype being the hash-keys of (fmp/failure-handlers fmp)
          using (hash-value fh)
          when (typep err ftype) return fh)))

(defmacro failure-handler-dispatcher (fmp err default-action &key (spec-handler nil) (failures nil))
  "Look in fmp for a handler for the failure and depending on its return value, retry, resignal, or return
   normally. If no handler, use default behavior. Default behavior uses either err or a longer collection
   of previous failures if that exists."
  `(let* ((handler (find-handler ,fmp ,err))
          (default-resig (if (and ,failures (< 1 (length ,failures)))
                           (make-condition 'composite-failure :failures (reverse ,failures))
                           ,err)))
    (if handler
      (multiple-value-bind (action retval) (apply handler (list ,err :previous-failures ,failures))
        (cond 
          ((eq action 'ptr-return) (return retval))
          ((eq action 'ptr-retry) (retry))
          ((eq action 'ptr-fail) (fail retval))
          (T nil))))
    ;; If we're still here, then either a handler wasn't found or the action returned was
    ;; not among PTR-RETRY, PTR-FAIL, PTR-RETURN, in which case revert to default behavior.
    (if (and ,spec-handler (typep ,err (first ,spec-handler)))
      (multiple-value-bind (action retval) (apply (second ,spec-handler) (list ,err :previous-failures ,failures))
        (cond 
          ((eq action 'ptr-return) (return retval))
          ((eq action 'ptr-retry) (retry))
          ((eq action 'ptr-fail) (fail retval))
          (T nil))))
    ;; Again, if we're still here, then either a handler wasn't found or the action returned was
    ;; not among PTR-RETRY, PTR-FAIL, PTR-RETURN, in which case revert to default behavior.
    (cond
      ((eq ,default-action 'ptr-return) (return default-resig))
      ((eq ,default-action 'ptr-retry) (retry))
      (T (fail default-resig)))))

(defmacro with-ptr-goal-failure-handler ((goal-object default-action &key (spec-handler nil)) &body body)
  (with-gensyms (failures)
    `(block nil
       (let* ((,failures (list)))
         (with-transformative-failure-handling
           ((condition (err)
              (setf ,failures (cons err ,failures))
              (failure-handler-dispatcher ,goal-object err ,default-action :spec-handler ,spec-handler :failures ,failures)))
           ,@body)))))

(defun init-failure-handlers (fmp failure-handlers)
  (sb-thread:with-mutex ((fmp/mutex fmp))
    (clrhash (fmp/failure-handlers fmp))
    (loop for fh in failure-handlers do
      (setf (gethash (first fh) (fmp/failure-handlers fmp)) 
            (second fh)))))

(defun init-fluent-conditions (fmp fluent-conditions)
  (sb-thread:with-mutex ((fmp/mutex fmp))
    (clrhash (fmp/fluent-conditions fmp))
    (loop for fc in fluent-conditions do
      (setf (gethash (first fc) (fmp/fluent-conditions fmp)) 
            (second fc)))))

(defun extend-fc-hash (acc ext)
  (loop for key being the hash-keys of ext
        using (hash-value value)
        when (not (nth-value 1 (gethash key acc)))
        do (setf (gethash key acc) value)))
(defun construct-fluent-condition (goal goal-pattern goal-recipe)
  (sb-thread:with-mutex ((fmp/mutex goal))
    (sb-thread:with-mutex ((fmp/mutex goal-pattern))
      (sb-thread:with-mutex ((fmp/mutex goal-recipe))
        (let* ((goal-fcs (fmp/fluent-conditions goal))
               (gp-fcs (fmp/fluent-conditions goal-pattern))
               (gpr-fcs (fmp/fluent-conditions goal-recipe))
               (fcs (make-hash-table :test #'eq))
               (fls-list nil)
               (expvals-list nil))
          (extend-fc-hash fcs gpr-fcs)
          (extend-fc-hash fcs gp-fcs)
          (extend-fc-hash fcs goal-fcs)
          (setf fls-list
            (loop for key being the hash-keys of fcs collect key into R finally (return R)))
          (setf expvals-list
            (loop for key being the hash-keys of fcs using (hash-value value) collect value into R finally (return R)))
          (cons
            (apply #'fl-funcall
                   (cons
                     (lambda (&rest args)
                       (let* ((expvals (car args))
                              (fls (cdr args))
                              (have-failure nil))
                         (loop for fl in fls
                               for vl in expvals
                               when (not (equal (value fl) vl))
                               do (progn
                                   (setf have-failure T)
                                   (return)))
                         have-failure))
                     (cons
                       expvals-list
                       fls-list)))
            (cons
              expvals-list
              fls-list)))))))

(defun record-failed-fluents (fls-list expvals)
  (remove
    nil
    (mapcar
      (lambda (fl ev)
        (if (not (equal (value fl) ev))
          (list fl (value fl) ev)
          nil))
      fls-list
      expvals)))

(defun tried-recipe? (recipe tried-recipes)
  (loop for tr in tried-recipes
    when (equal (fmp/name recipe) (fmp/name tr)) do (return T)))

;; This assumes the list of recipes in goal-pattern is sorted by score.
(defun find-next-recipe (goal-pattern tried-recipes)
  (sb-thread:with-mutex ((fmp/mutex goal-pattern))
    (loop for recipe in (gp/recipes goal-pattern)
      when (not (tried-recipe? recipe tried-recipes)) do (return recipe))))

(defun find-ptr-goal-pattern (goal args)
  (sb-thread:with-mutex ((fmp/mutex goal))
    (loop for goal-pattern in (goal/patterns goal) do
      (progn
        (multiple-value-bind (bdgs ok?) (cut:pat-match (gp/pattern goal-pattern) args)
          (if ok?
            (return (values goal-pattern bdgs))))))))

(defun insert-goal-pattern-internal (gp-list gp &key (overwrite T))
  (let* ((cgp (car gp-list))
         (right-place (and cgp (equal (gp/pattern gp) (gp/pattern cgp)))))
    (if right-place
      (if overwrite
        (setf (car gp-list) gp)
        cgp)
      (if cgp
        (insert-goal-pattern-internal (cdr gp-list) gp :overwrite overwrite)))))
(defun insert-goal-pattern (goal goal-pattern &key (overwrite T))
  (sb-thread:with-mutex ((fmp/mutex goal))
    (let* ((ret-gp (insert-goal-pattern-internal (goal/patterns goal) goal-pattern :overwrite overwrite)))
      (if ret-gp
        ret-gp
        (progn
          (setf (goal/patterns-w goal) (nconc (goal/patterns goal) (list goal-pattern)))
          goal-pattern)))))

(defun ensure-gp (goal lambda-list)
  (let* ((gp (make-instance
               'ptr-goal-pattern
               :body (lambda (&rest args) (declare (ignore args)) nil)
               :parent goal
               :pattern lambda-list)))
    (insert-goal-pattern goal gp :overwrite nil)))

(defun replace-recipe-by-name (gpr-list gpr &key (only-update-score nil))
  (let* ((cgpr (car gpr-list)))
    (if cgpr
      (sb-thread:with-mutex ((fmp/mutex cgpr))
        (if (equal (fmp/name cgpr) (fmp/name gpr))
          (if only-update-score
            (progn 
              (setf (gpr/score cgpr) (gpr/score gpr))
              ;; just to keep return type consistent on all branches: return a goal-pattern-recipe (or nil)
              cgpr)
            (setf (car gpr-list) gpr))
          (replace-recipe-by-name (cdr gpr-list) gpr :only-update-score only-update-score)))
      nil)))
(defun update-goal-pattern-recipe (gp gpr &key (only-update-score nil))
  (sb-thread:with-mutex ((fmp/mutex gp))
    (let* ((replaced-by-name (replace-recipe-by-name (gp/recipes gp) gpr :only-update-score only-update-score)))
      (if replaced-by-name
        (setf (gp/recipes gp) (sort (gp/recipes gp) #'> :key #'gpr/score))
        (if only-update-score
          nil
          (setf (gp/recipes gp) (merge 'list (gp/recipes gp) (list gpr) #'> :key #'gpr/score)))))))

(defmacro create-recipe-task (task-name parameters path-part body)
  `(make-instance 'task
                  :name ,task-name
                  :thread-fun (lambda ()
                                (with-task-tree-node
                                  (:path-part ,path-part
                                   :name ,task-name
                                   :sexp (,task-name () (apply ,body ,parameters))
                                   :lambda-list ()
                                   :parameters ())
                                  (apply ,body ,parameters)))))

(defun run-ptr-goal-recipe (goal goal-pattern goal-recipe bdgs)
  (with-ptr-goal-failure-handler (goal-recipe 'ptr-fail)
    (let* ((fc-aux (construct-fluent-condition goal goal-pattern goal-recipe)) ;; returns (fluent-condition expvals-list mon.fl1 mon.fl2 ...)
           (fluent-condition (car fc-aux))
           (expvals (cadr fc-aux))
           (fls-list (cddr fc-aux))
           (body (fmp/body goal-recipe))
           (task-name (format nil "Goal-recipe-~a" (fmp/name goal-recipe)))
           (pattern (gp/pattern goal-pattern))
           (lambda-list (reverse (cut:vars-in pattern))) ;; cut:vars-in reverses the order of appearance of variables, which is a bit ... counter-intuitive.
           (parameters (mapcar (alexandria:rcurry #'cut:var-value bdgs) lambda-list))
           (path-part (list 'goal (cons (fmp/name goal-recipe) pattern)))
           (recipe-task (create-recipe-task task-name parameters path-part body))
           (recipe-status (status recipe-task))
           (recipe-done-fluent (fl-funcall (lambda (fluent-condition recipe-status)
                                             (or (value fluent-condition) 
                                                 (equal (value recipe-status) :succeeded) 
                                                 (equal (value recipe-status) :failed)
                                                 (equal (value recipe-status) :evaporated)))
                                           fluent-condition
                                           recipe-status)))
    (wait-for recipe-done-fluent)
    (if (value fluent-condition)
      (progn
        (evaporate recipe-task)
        (fail 'fluent-condition-failure :failed-fluents (record-failed-fluents fls-list expvals))))
    (if (equal (value recipe-status) :failed)
      (fail (result recipe-task))
      (result recipe-task)))))

(defun run-ptr-goal-pattern (goal goal-pattern bdgs)
  (sb-thread:with-mutex ((fmp/mutex goal-pattern))
    (if (fmp/body goal-pattern)
      (let* ((pattern (gp/pattern goal-pattern))
             (lambda-list (reverse (cut:vars-in pattern))) ;; cut:vars-in reverses the order of appearance of variables, which is a bit ... counter-intuitive.
             (parameters (mapcar (alexandria:rcurry #'cut:var-value bdgs) lambda-list)))
        (apply (fmp/body goal-pattern) parameters))))
  (let* ((tried-recipes nil)
         (pattern (gp/pattern goal-pattern))
         (tried-tail nil))
    (with-ptr-goal-failure-handler (goal-pattern 'ptr-retry 
                                    :spec-handler (list 'goal-recipes-failed 
                                                        (lambda (err &key (previous-failures nil))
                                                          (declare (ignorable err)) 
                                                          (values 'ptr-fail (make-condition 'composite-failure :failures (reverse previous-failures))))))
      (let* ((next-recipe (find-next-recipe goal-pattern tried-recipes)))
        (unless next-recipe
          ;; Reset tried-recipes, just in case some user-defined handler for goal-recipes-failed decides to retry.
          ;; (There probably shouldn't be user handlers for this signal, but we'll leave it in.)
          (setf tried-recipes nil)
          (setf tried-tail nil)
          (fail 'goal-recipes-failed :goal goal :pattern pattern))
        ;; Rather than the usual (cons new-element list), we prefer to add elements to the tail of the list via the tail-tracking trick.
        ;; Reason is, we'd rather search tried recipes in order of best-tried so far to last, to hopefully minimize comparisons.
        ;; (And suspect that tail-tracking is more efficient than reversing for every search).
        (if tried-recipes
          (progn
            (setf (cdr tried-tail) (cons next-recipe nil))
            (setf tried-tail (cdr tried-tail)))
          (progn
            (setf tried-recipes (cons next-recipe nil))
            (setf tried-tail tried-recipes)))
        (setf tried-recipes (cons next-recipe tried-recipes))
        (run-ptr-goal-recipe goal goal-pattern next-recipe bdgs)))))

(defmacro ptr-declare-goal ((name lambda-list &key (fluent-conditions nil) (failure-handlers nil)) &body body)
  "Declare a ptr-goal: a logical collection of functions meant to achieve some plan goal.
   Collection is empty after the execution of this function, and must be extended with
   PTR-ADD-GOAL-RECIPE and/or adjusted with PTR-ADJUST-RECIPE-SCORE.

   Goals can only be called inside CRAM plans.

   FLUENT-CONDITIONS is a list of pairs: (fluent-object expected-value). A fluent object should
   only appear once in the list (or, if it appears more, have EQUAL expected-value). During
   execution of the goal or one of its subsumed recipes, the fluent-object must maintain the
   expected value, or else a FLUENT-CONDITION-FAILURE is signalled.

   If a recipe subsumed by the goal defines a different expected-value for the fluent, the
   recipe's expected value will be used instead while running that particular recipe.

   FAILURE-HANDLERS is a list of (type function) pairs where TYPE is a symbol describing a 
   condition type and FUNCTION is a function that takes one parameter of condition type
   and a key parameter :previous-failures.
   Conditions issued by the goal or one of its subsumed recipes (including FLUENT-CONDITION-FAILURE)
   can be caught by these handlers.

   A subsumed recipe can define a handler for a condition that its parent goal also has a
   handler for. In this case, the recipe's handle gets called first. If it doesn't resolve
   the condition, the goal's handler is then called.

   FAILURE-HANDLERS should return two values: one is an action to take as a result of handling,
   the second is an optional return value if the goal is supposed to return rather than
   propagate a signal upwards.

   Actions to take after handling:
     - 'PTR-RETRY: retry the body of the goal from scratch
     - 'PTR-RETURN: return normally (return the value given by the failure handler)
     - 'PTR-FAIL: propagate condition upwards (default). If the failure handler returns a value,
       this should be of condition type and it will be signalled upwards."
  (multiple-value-bind (forms declarations doc-string)
      (parse-body body :documentation t)
    (with-gensyms (args)
      `(progn
         (defparameter ,name
                       (make-instance 'ptr-goal
                                      :name (format nil "~a" ',name)
                                      :body (lambda ,lambda-list ,@declarations (with-tags ,@forms))))
         (def-ptr-cram-function ,name (&rest ,args)
           ,doc-string
           (with-ptr-goal-failure-handler (,name 'ptr-fail)
             (flet ((before ,lambda-list
                      ,@declarations
                      ,@forms))
               (apply #'before ,args))
             (multiple-value-bind (goal-pattern bdgs) (find-ptr-goal-pattern ,name ,args)
               (unless goal-pattern
                 (fail 'goal-pattern-not-found :goal ,name :pattern ,args))
               (run-ptr-goal-pattern ,name goal-pattern bdgs))))
         (setf (goal/function-w ,name) #',name)
         (init-failure-handlers ,name ,failure-handlers)
         (init-fluent-conditions ,name ,fluent-conditions)))))

(defmacro ptr-add-goal-pattern ((goal lambda-list &key (fluent-conditions nil) (failure-handlers nil)) &body body) 
  (multiple-value-bind (forms declarations)
      (parse-body body :documentation nil)
    (with-gensyms (gp)
      `(let* ((,gp (make-instance 
                     'ptr-goal-pattern
                     :pattern ',lambda-list
                     :body (lambda ,lambda-list ,@declarations (with-tags ,@forms))
                     :parent ,goal)))
         (init-failure-handlers ,gp ,failure-handlers)
         (init-fluent-conditions ,gp ,fluent-conditions)
         (insert-goal-pattern ,goal ,gp)))))

(defmacro ptr-add-goal-recipe ((goal pattern name score &key (fluent-conditions nil) (failure-handlers nil)) &body body)
  (multiple-value-bind (forms declarations)
      (parse-body body :documentation nil)
    (let* ((lambda-list (reverse (cut:vars-in pattern))))
      (with-gensyms (gp gpr)
        `(let* ((,gp (ensure-gp ,goal ',pattern))
                (,gpr (make-instance 
                        'ptr-goal-pattern-recipe
                        :name ,name
                        :score ,score
                        :body (lambda ,lambda-list ,@declarations (with-tags ,@forms))
                        :parent ,gp)))
           (init-failure-handlers ,gpr ,failure-handlers)
           (init-fluent-conditions ,gpr ,fluent-conditions)
           (update-goal-pattern-recipe ,gp ,gpr))))))

(defun ptr-adjust-recipe-score (goal lambda-list name score) 
  (let* ((gp (ensure-gp goal lambda-list)) ;; todo: replace with a find pattern here.
         (dummy-gpr (make-instance 
                      'ptr-goal-pattern-recipe
                      :name name
                      :score score)))
    (if (update-goal-pattern-recipe gp dummy-gpr :only-update-score T)
      T
      (fail 'goal-recipe-not-found :name name :pattern lambda-list :goal goal))))

(defun ptr-remove-recipe (goal lambda-list name)
  (let* ((gp (ensure-gp goal lambda-list))) ;; todo: replace with a find pattern here.
    (sb-thread:with-mutex ((fmp/mutex gp))
      (setf (gp/recipes gp) (delete-if (lambda (a)
                                           ;; Will never change name in a recipe, so no need for mutexes here.
                                           (equal (fmp/name a) name)) 
                                         (gp/recipes gp))))))

(defun ptr-clear-patterns (goal)
  (setf (goal/patterns-w goal) nil))
