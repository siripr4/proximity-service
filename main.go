package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/labstack/echo/v4"
)

type Response struct {
	Message string `json:"message"`
}

var port = 8080

func main() {
	e := echo.New()
	e.HideBanner = true

	routes(e)

	log.Fatal(e.Start(fmt.Sprintf(":%d", port)))
}

func routes(e *echo.Echo) {
	e.GET("/ping", func(c echo.Context) error {
		return c.JSON(http.StatusOK, Response{Message: "pong"})
	})
	e.GET("/", func(c echo.Context) error {
		return c.JSON(http.StatusOK, Response{Message: "proximity service"})
	})

	// v1 routes
	v1 := e.Group("/v1")
	v1.Add("GET", "/nearby/search", nearbySearch)

	v1.Add("GET", "/businesses/{:id}", getBusiness)
	v1.Add("POST", "/businesses/", addBusiness)
	v1.Add("PUT", "/business", updateBusiness)
	v1.Add("GET", "/business/{:id}", deleteBusiness)

}

func nearbySearch(c echo.Context) error {
	fmt.Println("in nearby search handler")
	return c.JSON(http.StatusOK, Response{Message: "TODO"})
}

// Refer the following APIs to design business APIs
// Google places API: https://developers.google.com/maps/documentation/places/web-service/search 
// Yelp business endpoints: https://www.yelp.com/developers/documentation/v3/business_search


func getBusiness(c echo.Context) error {
	fmt.Println("in get business handler")
	return c.JSON(http.StatusOK, Response{Message: "TODO"})
}

func addBusiness(c echo.Context) error {
	fmt.Println("in post business handler")
	return c.JSON(http.StatusOK, Response{Message: "TODO"})
}

func updateBusiness(c echo.Context) error {
	fmt.Println("in put business handler")
	return c.JSON(http.StatusOK, Response{Message: "TODO"})
}

func deleteBusiness(c echo.Context) error {
	fmt.Println("in delete business handler")
	return c.JSON(http.StatusOK, Response{Message: "TODO"})
}
