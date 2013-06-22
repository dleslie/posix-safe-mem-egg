(module posix-safe-mem *

        (import scheme chicken)
        (use posix lolevel posix-shm posix-semaphore uuid)

        ;; Store PSHM object
        (define-record safe-mem 
          semaphore semaphore-name memory-size shared-memory shared-memory-path memory-map memory-map-protection memory-map-flags copy grow shrink pass-count)

        (define-record-printer (safe-mem m p)
          (fprintf p "#,<safe-mem semaphore: ~S semaphore-name: ~S memory-size: ~S shared-memory: ~S shared-memory-path: ~S memory-map: ~S copy: ~S grow: ~S shrink: ~S pass-count:~S>"
                   (safe-mem-semaphore m) (safe-mem-semaphore-name m) (safe-mem-memory-size m) (safe-mem-shared-memory m) (safe-mem-shared-memory-path m) (safe-mem-memory-map m) (safe-mem-copy m) (safe-mem-grow m) (safe-mem-shrink m) (safe-mem-pass-count m)))

        (define (lock-safe-mem safe-mem)
          (when (>= 0 (safe-mem-pass-count safe-mem))
                (sem-wait (safe-mem-semaphore safe-mem)))
          (safe-mem-pass-count-set! safe-mem (+ 1 (safe-mem-pass-count safe-mem))))

        (define (unlock-safe-mem safe-mem)
          (when (< 0 (safe-mem-pass-count safe-mem))
                (safe-mem-pass-count-set! safe-mem (- 1 (safe-mem-pass-count safe-mem))))
          ;; pass-count does not reside in shared memory, and so is a process-local variable
          (when (>= 0 (safe-mem-pass-count safe-mem))
                (sem-post (safe-mem-semaphore safe-mem))))

        (define-syntax with-safe-mem
          (syntax-rules ()
            ((with-safe-mem safe-mem body ...)
             (dynamic-wind
                 (lambda () (lock-safe-mem safe-mem))
                 (lambda () body ...)
                 (lambda () (unlock-safe-mem safe-mem))))))

        ;; get: lambda returns value, using unevict if copy and pointer->object if not
        (define (safe-mem-get safe-mem)
          (let ((primary (pointer->object (memory-mapped-file-pointer (safe-mem-memory-map safe-mem)))))
            (if (safe-mem-copy safe-mem)
                (object-unevict primary #t)
                primary)))

        (define (safe-mem-set! safe-mem value)
          (assert (< 0 (object-size value)) "Shared object size must be greater than zero. (Did you try to share an immediate object?)")
          ;; Check if mmap or resize is necessary
          (when (or (not (safe-mem-memory-map safe-mem)) 
                    (> (object-size value) (safe-mem-memory-size safe-mem)))
                (let ((remap (not (safe-mem-memory-map safe-mem))))
                  ;; Set the new size if necessary
                  (when (or (and (safe-mem-grow safe-mem) (> (object-size value) (safe-mem-memory-size safe-mem)))
                            (and (safe-mem-shrink safe-mem) (< (object-size value) (safe-mem-memory-size safe-mem))))
                        (safe-mem-memory-size-set! safe-mem (object-size value))
                        (set! remap #t))
                  ;; Resize the pshm object if necessary
                  (safe-mem-truncate! safe-mem)
                  ;; Remap the memory map if any resizing occurred, or if we're initializing
                  (when remap
                        ;; Unmap if we're resizing and not initializing
                        (when (safe-mem-memory-map safe-mem)
                              (unmap-file-from-memory (safe-mem-memory-map safe-mem)))
                        ;; mmap ahoy
                        (let ((mmap (map-file-to-memory #f (safe-mem-memory-size safe-mem) 
                                                        (safe-mem-memory-map-protection safe-mem) 
                                                        (safe-mem-memory-map-flags safe-mem) 
                                                        (safe-mem-shared-memory safe-mem))))
                          (safe-mem-memory-map-set! safe-mem mmap)))))

          ;; Write the new value to the location
          (receive (v p) 
                   (object-evict-to-location value (memory-mapped-file-pointer (safe-mem-memory-map safe-mem)) (safe-mem-memory-size safe-mem))
                   v))

        (define (safe-mem-get/lock safe-mem)
          (with-safe-mem safe-mem (safe-mem-get safe-mem)))

        (define (safe-mem-set!/lock safe-mem value)
          (with-safe-mem safe-mem (safe-mem-set! safe-mem value)))

        (define (safe-mem-truncate! safe-mem #!optional (size #f))
          (when (not size)
                (set! size (safe-mem-memory-size safe-mem)))
          (file-truncate (safe-mem-shared-memory safe-mem) size))

        (define make-safe-mem
          (let ((%make make-safe-mem))
            (lambda (value 
                     #!key (semaphore-name (sprintf "/safe-mem-semaphore-~A" (uuid-v4)))
                     (shared-memory-name (sprintf "/safe-mem-shared-memory-~A" (uuid-v4)))
                     (shared-memory-flags (list open/creat open/rdwr))
                     (semaphore-flags o/creat)
                     (semaphore-mode 0644)
                     (size (object-size value))
                     (memory-map-protection (bitwise-ior prot/read prot/write prot/exec))
                     (memory-map-flags map/shared)
                     (copy #t)
                     (grow #t)
                     (shrink #t))
              (assert (< 0 size) "Shared object size must be greater than zero. (Did you try to share an immediate object?)")
              (let* ((mmap #f)
                     (pass-count 0)
                     (safe-mem (%make (sem-open/mode semaphore-name semaphore-flags semaphore-mode 1)
                                     semaphore-name
                                     size
                                     (shm-open shared-memory-name shared-memory-flags)
                                     shared-memory-name
                                     mmap
                                     memory-map-protection
                                     memory-map-flags
                                     copy
                                     grow
                                     shrink
                                     pass-count)))
                ;; safe-mem-set! handles the initialization of the mmap, as well as writing the value
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
           a 
           (with-safe-mem 
            b
            (if (safe-mem-copy a)
                (let* ((t (safe-mem-get a))
                       (t2 (safe-mem-get b)))
                  (safe-mem-set! a (if (safe-mem-copy b) t2 (object-evict t2)))
                  (safe-mem-set! b t))
                (let* ((t (object-evict (safe-mem-get a)))
                       (t2 (safe-mem-get b)))
                  (safe-mem-set! a (if (safe-mem-copy b) t2 (object-evict t2)))
                  (safe-mem-set! b t)))))
          #t)
)
