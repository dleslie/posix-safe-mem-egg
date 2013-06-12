(module posix-safe-mem *

        (import scheme chicken)
        (use posix lolevel posix-shm posix-semaphore uuid)

        ;; Store PSHM object
        (define-record safe-mem 
          semaphore semaphore-name memory-size shared-memory shared-memory-path memory-map copy owner-pid pass-count)

        (define-record-printer (safe-mem m p)
          (fprintf p "#,<safe-mem semaphore: ~S semaphore-name: ~S memory-size: ~S shared-memory: ~S shared-memory-path: ~S memory-map: ~S copy: ~S>"
                   (safe-mem-semaphore m) (safe-mem-semaphore-name m) (safe-mem-memory-size m) (safe-mem-shared-memory m) (safe-mem-shared-memory-path m) (safe-mem-memory-map m) (safe-mem-copy m)))

        (define (lock-safe-mem safe-mem)
          (when (not (eq? (current-process-id) (safe-mem-owner-pid safe-mem)))
                (sem-wait (safe-mem-semaphore safe-mem))
                (safe-mem-owner-pid-set! safe-mem (current-process-id)))
          (safe-mem-pass-count-set! safe-mem (+ 1 (safe-mem-pass-count safe-mem))))

        (define (unlock-safe-mem safe-mem)
          (when (eq? (current-process-id) (safe-mem-owner-pid safe-mem))
                (safe-mem-pass-count-set! safe-mem (+ 1 (safe-mem-pass-count safe-mem)))
                (when (eq? 0 (safe-mem-pass-count safe-mem))
                      (safe-mem-owner-pid-set! safe-mem #f)
                      (sem-post (safe-mem-semaphore safe-mem)))))

        ;; Copy to/from PSHM
        ;; Second semaphore, inc on passing first, dec on done, if can't dec, then post first.
        (define-syntax with-safe-mem
          (syntax-rules ()
            ((with-safe-mem () body1 body2 ...)
             (begin body1 body2 ...))
            ((with-safe-mem (safe-mem1 safe-mem2 ...) body1 body2 ...)
             (dynamic-wind
                 (lambda ()
                   (lock-safe-mem safe-mem1))
                 (lambda ()
                   (with-safe-mem (safe-mem2 ...) body1 body2 ...))
                 (lambda ()
                   (unlock-safe-mem safe-mem1))))))

        ;; get: lambda returns value, using unevict if copy and pointer->object if not
        (define (safe-mem-get safe-mem)
          (let ((primary (pointer->object (memory-mapped-file-pointer (safe-mem-memory-map safe-mem)))))
            (if (safe-mem-copy safe-mem)
                (object-unevict primary)
                primary)))

        ;; setter: lambda sets value, using evict-object; if object-size > size then resize memory first
        ;; TODO: proper copy
        (define (safe-mem-set! safe-mem value)
          ;; Check if mmap or resize is necessary
          (when (or (not (safe-mem-memory-map safe-mem)) 
                    (> (object-size value) (safe-mem-memory-size safe-mem)))
                ;; Unmap if we're resizing
                (when (safe-mem-memory-map safe-mem)
                      (unmap-file-from-memory (safe-mem-memory-map safe-mem)))
                ;; Set the new size if necessary
                (when (> (object-size value) (safe-mem-memory-size safe-mem))
                      (safe-mem-memory-size-set! safe-mem (object-size value)))
                ;; Resize the pshm object
                (file-truncate (safe-mem-shared-memory safe-mem) (safe-mem-memory-size safe-mem))
                ;; mmap ahoy
                (let ((mmap (map-file-to-memory #f (safe-mem-memory-size safe-mem) (bitwise-ior prot/read prot/write prot/exec) map/shared (safe-mem-shared-memory safe-mem))))
                  (safe-mem-memory-map-set! safe-mem mmap)))
          ;; Write the new value to the location
          (receive (v p) 
                   (object-evict-to-location value (memory-mapped-file-pointer (safe-mem-memory-map safe-mem)) (safe-mem-memory-size safe-mem))
                   v))

        ;; Create PSHM object
        ;; Want optional size
        (define make-safe-mem
          (let ((%make make-safe-mem))
            (lambda (value 
                     #!key (semaphore-name (sprintf "/safe-mem-semaphore-~A" (uuid-v4)))
                     (shared-memory-name (sprintf "/safe-mem-shared-memory-~A" (uuid-v4)))
                     (size (object-size value))
                     (copy #t))
              (let ((safe-mem (%make (sem-open/mode semaphore-name o/creat 0644 1)
                                     semaphore-name
                                     size
                                     (shm-open shared-memory-name (list open/creat open/rdwr))
                                     shared-memory-name
                                     #f
                                     copy
                                     #f
                                     0
                                     )))
                (safe-mem-set! safe-mem value)
                safe-mem))))

        (define (free-safe-mem! safe-mem)
          (and
           (when (safe-mem-memory-map safe-mem)
                 (unmap-file-from-memory (safe-mem-memory-map safe-mem)))
           (when (safe-mem-shared-memory safe-mem)
                 (shm-unlink (safe-mem-shared-memory-path safe-mem)))
           (when (safe-mem-semaphore safe-mem)
                 (and
                  (sem-close (safe-mem-semaphore safe-mem))
                  (sem-unlink (safe-mem-semaphore-name safe-mem))))
           #t))

        (define (swap-safe-mem! a b)
          (with-safe-mem 
           (a b)
           (if (safe-mem-copy a)
               (let* ((t (safe-mem-get a))
                      (t2 (safe-mem-get b)))
                 (safe-mem-set! a (if (safe-mem-copy b) t2 (object-evict t2)))
                 (safe-mem-set! b t))
               (let* ((t (object-evict (safe-mem-get a)))
                      (t2 (safe-mem-get b)))
                 (safe-mem-set! a (if (safe-mem-copy b) t2 (object-evict t2)))
                 (safe-mem-set! b t)))))
)
