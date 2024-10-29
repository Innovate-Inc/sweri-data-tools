library(shiny)
library(shinyWidgets)
library(datamods)
library(MASS)
library(tidyverse)
library(plotly)
library(networkD3)
library(RColorBrewer)
library(sortable)

# define "not in" operator
`%ni%` <- Negate(`%in%`)

# read in non-hf facts data
# from tidy_facts.R
# rule 0 (gis_acres > 5) is already applied
facts_all <- read_csv("tidy_facts.csv")
# ~ 5 million records, 9 columns

# lists of 'valid' methods and equipment
logging_methods <- read_csv("logging_methods.csv") %>%
  filter(include == TRUE) %>%
  pull(method)
logging_equip <- read_csv("logging_equipment.csv") %>%
  filter(include == TRUE) %>%
  pull(equipment)
fire_methods <- read_csv("fire_methods.csv") %>%
  filter(include == TRUE) %>%
  pull(method)
fire_equip <- read_csv("fire_equipment.csv") %>%
  filter(include == TRUE) %>%
  pull(equipment)
fuel_methods <- read_csv("fuel_methods.csv") %>%
  filter(include == TRUE) %>%
  pull(method)
fuel_equip <- read_csv("fuel_equipment.csv") %>%
  filter(include == TRUE) %>%
  pull(equipment)
other_methods <- read_csv("other_methods.csv") %>%
  filter(include == TRUE) %>%
  pull(method)
other_equip <- read_csv("other_equipment.csv") %>%
  filter(include == TRUE) %>%
  pull(equipment)
special_exclusions <- read_csv("special_exclusions.csv") %>%
  filter(include == TRUE) %>%
  pull(activity)


facts_all <- facts_all %>%
  mutate(
    # rule 2 ####
    # logging-associated activities are conditionally included
    r2 = case_when(
      str_detect(activity, "Thin|thin|Cut|cut") &
        (method %in% logging_methods | is.na(method)) &
        (equipment %in% logging_equip | is.na(equipment))
      ~ "PASS",
      .default = "FAIL"
    ),
    # rule 3 ####
    # fire-associated activities are conditionally included
    r3 = case_when(
      str_detect(activity, "Burn|burn|Fire|fire") &
        (method %in% fire_methods | is.na(method)) &
        (equipment %in% fire_equip | is.na(equipment))
      ~ "PASS",
      .default = "FAIL"
    ),
    # rule 4 ####
    # fuel-associated activities are conditionally included
    r4 = case_when(
      str_detect(activity, "Fuel|fuel") &
        (method %in% fuel_methods | is.na(method)) &
        (equipment %in% fuel_equip | is.na(equipment))
      ~ "PASS",
      .default = "FAIL"
    ),
    # rule 5 ####
    # other activities are conditionally included. method AND equipment are
    # required.
    r5 = case_when(
      !str_detect(activity, "Thin|thin|Cut|cut|Burn|burn|Fire|fire") &
        !is.na(method) & !is.na(equipment) &
        method != "No method" & equipment != "No equipment" &
        method %in% other_methods &
        equipment %in% other_equip
      ~ "PASS",
      .default = "FAIL"
    ),
    # rule 6 ####
    # some logging, fire, and fuels-related activities with NA methods and
    # equipment sneak through the filters. these activities require manual
    # exclusion.
    r6 = case_when(
      (r2 == "PASS" | r3 == "PASS" | r4 == "PASS") &
        activity %in% special_exclusions
      ~ "PASS",
      .default = "FAIL"
    ),

    # final determination of inclusion
    included = case_when(
      ((r2 == "PASS" | r3 == "PASS" | r4 == "PASS") & r6 == "PASS") |
        (r5 == "PASS")
      ~ "YES",
      .default = "NO"
    )
  )

# tidy data for sankey diagram
facts <- facts_all %>%
  # select only the relevant columns
  select(
    activity, method, equipment, r2, r3, r4, r5, r6, included, acres = gis_acres
  ) %>%
  # clean up method and equipment columns
  # values must be unqiue: can't have same value in both methods and equipment
  # or Sankey diagram will break
  mutate(
    method = case_when(method == "Not Applicable" ~ "No method",
                       is.na(method) ~ "No method",
                       method == "Highlead" ~ "Highlead method",
                       method == "Mobile Ground" ~ "Mobile Ground method",
                       .default = method),
    equipment = case_when(equipment == "Not Applicable" ~ "No equipment",
                          is.na(equipment) ~ "No equipment",
                          equipment == "Highlead" ~ "Highlead equipment",
                          equipment == "Mobile Ground" ~
                            "Mobile Ground equipment",
                          .default = equipment),
  )

# define labels for sorting sankey columns
labels <- c("activity", "method", "equipment", "included")


## shiny ui ####

ui <- fluidPage(

  tags$h2("Explore FACTS Data"),
  tags$h3("v1.0 2024/10/28"),
  actionButton("saveFilterButton", "Save Filter Values"),
  actionButton("loadFilterButton", "Load Filter Values"),
  radioButtons(
    inputId = "dataset",
    label = "Data:",
    choices = c(
      "facts"
    ),
    inline = TRUE
  ),

  filter_data_ui("filtering", max_height = NULL),

  rank_list_basic <- rank_list(
    text = "Sankey column order",
    labels = labels,
    input_id = "rank_list_basic"
  ),

  progressBar(
    id = "pbar", value = 100,
    total = 100, display_pct = TRUE
  ),

  plotlyOutput("sankey")

)

## shiny server ####

server <- function(input, output, session) {
  saved_filter_values <- reactiveVal()
  data <- reactive({
    facts
  })

  vars <- reactive({
    NULL
  })

  observeEvent(input$saveFilterButton, {
    saved_filter_values <<- res_filter$values()
  }, ignoreInit = T)

  defaults <- reactive({
    input$loadFilterButton
    saved_filter_values
  })

  res_filter <- filter_data_server(
    id = "filtering",
    drop_ids = FALSE,
    data = data,
    name = reactive(input$dataset),
    vars = vars,
    defaults = defaults,
    widget_num = "range",
    widget_date = "slider",
    widget_char = "picker",
    label_na = "Missing"
  )

  observeEvent(res_filter$filtered(), {
    updateProgressBar(
      session = session, id = "pbar",
      value = nrow(res_filter$filtered()), total = nrow(data())
    )
  })

  output$table <- reactable::renderReactable({
    reactable::reactable(res_filter$filtered())
  })

## ordered list ###
  varlist <- reactive({
    input$rank_list_basic
  })

  output$varlist <- renderPrint({
    varlist()
  })
  output$varlist_1 <- renderPrint({
    varlist()[1]
  })
  # sankey diagram
  output$sankey <- renderPlotly({


    data <- res_filter$filtered()
    vlist <- varlist()
    df_toplevel <- data %>%
      # group_by(activity, method) %>%
      group_by(!!sym(vlist[[1]]), !!sym(vlist[2])) %>%
      summarize(counts = n()) %>%
      ungroup()
    names(df_toplevel)[1] <- "Source Name"
    names(df_toplevel)[2] <- "Target Name"

    df_midlevel <- data %>%
      # group_by(method, equipment) %>%
      group_by(!!sym(vlist[2]), !!sym(vlist[3])) %>%
      summarize(counts = n()) %>%
      ungroup()
    names(df_midlevel)[1] <- "Source Name"
    names(df_midlevel)[2] <- "Target Name"

    # TODO: add additional levels. Make it possible to reorder the levels
    # df_midlowlevel <- data %>%
    #   group_by(equipment, fka) %>%
    #   summarize(counts = n()) %>%
    #   ungroup()
    # names(df_midlowlevel)[1] <- "Source Name"
    # names(df_midlowlevel)[2] <- "Target Name"

    df_lowlevel <- data %>%
      # group_by(equipment, included) %>%
      group_by(!!sym(vlist[3]), !!sym(vlist[4])) %>%
      summarize(counts = n()) %>%
      ungroup()
    # df_lowlevel <- data %>%
    #   group_by(fka, included) %>%
    #   summarize(counts = n()) %>%
    #   ungroup()
    names(df_lowlevel)[1] <- "Source Name"
    names(df_lowlevel)[2] <- "Target Name"

    # combine
    df_combined <- bind_rows(df_lowlevel, df_midlevel, df_toplevel)
    # df_combined <- bind_rows(df_lowlevel, df_midlowlevel,
    #                          df_midlevel, df_toplevel)



    ## create the keys that are in the levels above of type, family and code
    keys <- tibble(key = c(unique(data$activity), unique(data$method),
                           unique(data$equipment),
                           # unique(data$equipment), unique(data$fka),
                           unique(data$included)))

    # link in the Order which will get turned into the Target key or Source key
    keys <- keys %>% mutate(Order = 1:n())


    # join in Order using the keys. I'm using a named list to make that linkage
    # twice
    df_join1 <- left_join(df_combined, keys, by = c("Source Name" = "key")) %>%
      rename(`Source` = `Order`)

    df_join2 <- left_join(df_join1, keys, by = c("Target Name" = "key")) %>%
      rename(`Target` = `Order`)

    # plot using plotly so it is interactive
    plot_ly(
      type = "sankey",
      orientation = "h",
      node = list(
        label = c(0, keys$key), # added a zero as dummy because it is dropped.
                                # probably an issue with indexing?
        color = c(rep(brewer.pal(11, "Spectral"),
                      ceiling(length(keys$Order) / 11))),
        pad = 15,
        thickness = 20,
        line = list(
          color = "black",
          width = 0.5
        )
      ),
      link = list(
        source = c(df_join2$Source),
        target = df_join2$Target,
        value =  df_join2$counts
      )
    ) %>% layout(
      title = "Sankey Diagram",
      font = list(
        size = 10
      ),
      height = 1800
    )
  })

}

shinyApp(ui, server)
