package main

type InputError struct {
  Message string
}

func (err InputError) Error() string {
  return err.Message
}


