(use posix-safe-mem miscmacros)

(printf "Parent pid: ~S\n" (current-process-id))

;; Because object-evict is used to move data in and out of shared memory the records stored in safe-mem structures lose their type. Their type is restored if the safe-mem is set to copy (the default) or when if you were to use object-unevict on the returned data.
;; Using object-unevict isn't strictly necessary if you're comfortable with lolevel and homogenous vectors.
(define-record data (setter continue))

;; It's important to note that safe-data will not be of type <data>, but rather will be of type <safe-mem>.
;; The <data> record stored inside of the safe memory can be retrieved via safe-mem-get
(define safe-data (make-safe-mem (make-data #t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Test locking
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(process-fork 
 (lambda ()
   (printf "New child pid: ~S\n" (current-process-id))
   ;; Uses dynamic-wind to ensure that unlocking occurs nicely when the unexpected happens
   (with-safe-mem safe-data
                  (printf "Child process locked the semaphore\n")
                  (sleep 2))
   (printf "Child process unlocked the semaphore\n")))

(sleep 1)

(with-safe-mem safe-data
               (printf "Parent process locked the semaphore\n")
               (sleep 1))

(printf "Parent process unlocked the semaphore\n")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Test nesting
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(with-safe-mem safe-data
               (printf "Entered nesting level 0\n")
               (with-safe-mem safe-data
                              (printf "Entered nesting level 1\n"))
               (printf "Exiting nesting altogether\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Test reading/writing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(process-fork 
 (lambda ()
   (printf "New child pid: ~S\n" (current-process-id))
   (with-safe-mem safe-data
             (printf "Child process locked the semaphore\n")
             (sleep 1)
             ;; Since the copy flag was not specified, the default behaviour is to use object-unevict to return a managed copy of the data found in shared memory.
             ;; This is probably what you want, unless you're using shared memory for very large objects, and even so lock/copy/swap behaviour is served well by using the default safe-mem behaviour.
             (let ((data (safe-mem-get safe-data)))
               (set! (data-continue data) #f)
               (safe-mem-set! safe-data data))
             (printf "Child process flipped the continue bit\n"))
   (printf "Child process unlocked the semaphore\n")))

(printf "Parent process waiting on continue bit\n")

(let loop ()
  (sleep 1)
  ;; !!!
  ;; safe-mem-get and safe-mem-set! _do not_ lock the semaphore!
  ;; !!!
  ;; This behaviour is to allow users of this library to support a wide variety of reader/writer authority.
  ;; However, with-safe-mem returns the value of its body, so can be used as follows.
  ;; You could also use safe-mem-get/lock and safe-mem-set!/lock
  (let ((data (with-safe-mem safe-data (safe-mem-get safe-data))))
    (when (data-continue data)
          (loop))))

(printf "Parent process exiting\n")

(free-safe-mem! safe-data)
