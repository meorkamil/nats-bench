package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	firstNames = []string{"james", "emma", "liam", "olivia", "noah", "ava", "william", "sophia", "oliver", "isabella"}
	lastNames  = []string{"smith", "johnson", "williams", "brown", "jones", "garcia", "miller", "davis", "wilson", "moore"}
	domains    = []string{"gmail.com", "yahoo.com", "outlook.com", "proton.me", "icloud.com"}
)

func generateUser(seed string) User {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	first := firstNames[r.Intn(len(firstNames))]
	last := lastNames[r.Intn(len(lastNames))]
	domain := domains[r.Intn(len(domains))]

	// Use the seed variable to influence the email format
	var email string
	switch strings.ToLower(seed) {
	case "dot":
		email = fmt.Sprintf("%s.%s@%s", first, last, domain)
	case "underscore":
		email = fmt.Sprintf("%s_%s@%s", first, last, domain)
	case "number":
		email = fmt.Sprintf("%s%s%d@%s", first, last, r.Intn(9999), domain)
	default:
		email = fmt.Sprintf("%s%s@%s", first, last, domain)
	}

	caser := cases.Title(language.English)
	name := fmt.Sprintf("%s %s", caser.String(first), caser.String(last))

	return User{Name: name, Email: email}
}

func generateUsers(count int, seed string) []User {
	users := make([]User, count)
	for i := range users {
		users[i] = generateUser(seed)
	}
	return users
}
