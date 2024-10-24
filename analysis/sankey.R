library(shiny)
library(shinyWidgets)
library(datamods)
library(MASS)
library(tidyverse)
library(plotly)
library(networkD3)
library(RColorBrewer)

# define "not in" operator
`%ni%` <- Negate(`%in%`)

# read in all facts data
# from here: https://cfri.app.box.com/file/1638491149802
facts_all <- read_csv("tidy_facts.csv")

# rule 0: some activities are rejected immediately ####
# define list of exclusions
# excluded based on activity code alone - no inspection of polygons
exclusions <- c("Certification",
                "Reforestation Need Change",
                "Examination",
                "Prescription",
                "Diagnosis",
                "Exam",
                "Survey",
                "Analysis",
                "Delineation",
                "Monitoring",
                "Data",
                "(FIA)",
                "Inventory",
                "Permanent Plot",
                "Remote Sensing",
                "Administrative Changes",
                "Cruising",
                "Layout and Design",
                "Cone Collection",
                "Seed Collection",
                "seed collecting",
                "Seed Storage",
                "Seed Extraction",
                "Pollen",
                "Scion",
                "Cooler",
                "Activity Review",
                "TSI Need",
                "Fences")

# apply rule 0 filter and tidy data for sankey diagram
facts <- facts_all %>%
  # apply Rule 0
  filter(!str_detect(activity, str_c(exclusions, collapse = "|"))) %>%
  # select only the relevant columns
  select(activity, method, equipment, fka, acres) %>%
  # clean up method, equipment, and fka (fuels keypoint area) columns
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
    fka = as.character(fka)
  )

# identify categories with low counts (see Rule 4)
rare_a <- facts %>% count(activity) %>% filter(n < 20)
rare_m <- facts %>% count(method) %>% filter(n < 20)
rare_e <- facts %>% count(equipment) %>% filter(n < 20)

# # check included/excluded tables after applying Rule 0
# facts_excluded <- facts_all %>%
#  filter(str_detect(activity, str_c(exclusions, collapse = "|")))
# facts %>% count(activity) %>% arrange(desc(n)) %>% print(n = 100)
# facts_excluded %>% count(activity) %>% arrange(desc(n)) %>% print(n = 100)

# Apply rules 1:10
facts <- facts %>%
  mutate(
    included =
      case_when(

        # rule 1: fka 3 and 6 always in ####
        fka %in% c("3", "6") ~ "YES",

        # rule 2: must have acres >= 10 ####
        acres <= 10 | is.na(acres) ~ "NO",

        # rule 3: No method and equipment, no inclusion (some exceptions) ####
        method == "No method" &
          equipment == "No equipment" &
          !str_detect(activity, "Thin|thin|Cut|cut|Burn|burn|Fuel|fuel") ~ "NO",

        # rule 4: must not be a rare activity, method, or equipment ####
        activity %in% rare_a$activity ~ "NO",
        method %in% rare_m$method ~ "NO",
        equipment %in% rare_e$equipment ~ "NO",

        # rule 5: Wildfire is always out ####
        str_detect(activity, "Wildfire") ~ "NO",

        # rule 6: Fish stuff is always out, unless its logging ####
        str_detect(activity, "Fish") & !str_detect(equipment, "logging") ~ "NO",

        # rule 7: Clearcut almost always in
        str_detect(activity, "Clearcut") &
          !str_detect(method, "Inventory|Designation|Marking") ~ "YES",

        # rule 8: Burning activities are in, if method checks out ####
        str_detect(activity, "Burn|burn") &
          str_detect(method, "Fire|Manual|No method") ~ "YES",

        # rule 9: equipment that burns stuff is in ####
        equipment %in% c("Drip Torch", "Terra torch", "Verray torch",
                         "Ping pong balls", "Aerial ignition device",
                         "Air Curtain Incinerator") ~ "YES",

        # rule 10: "Thin" activity is in, barring weird methods/equipment ####
        str_detect(activity,
                   "Commercial Thin|Precommercial Thin| Precommercial thin") &
          method %in% c("Manual", "No method", "Power hand", "Mechanical",
                        "Logging Methods", "Tractor Logging") &
          equipment %in% c("No equipment", "Chain saw", "Feller Buncher",
                           "Rubber tired skidder logging", "Tractor logging")
        ~ "YES",

        # everything else: undecided ####
        .default = "undecided"
      ))

## shiny ui ####

ui <- fluidPage(

  tags$h2("Explore FACTS Data"),
  actionButton("saveFilterButton","Save Filter Values"),
  actionButton("loadFilterButton","Load Filter Values"),
  radioButtons(
    inputId = "dataset",
    label = "Data:",
    choices = c(
      "facts"
    ),
    inline = TRUE
  ),

  filter_data_ui("filtering", max_height = NULL),

  progressBar(
    id = "pbar", value = 100,
    total = 100, display_pct = TRUE
  ),

  plotlyOutput("sankey")

)

## shiny server ####

server <- function(input, output, session) {
  savedFilterValues <- reactiveVal()
  data <- reactive({
    facts
  })

  vars <- reactive({
      NULL
  })

  observeEvent(input$saveFilterButton,{
    savedFilterValues <<- res_filter$values()
  },ignoreInit = T)

  defaults <- reactive({
    input$loadFilterButton
    savedFilterValues
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

  # sankey diagram
  output$sankey <- renderPlotly({

    data <- res_filter$filtered()
    df_toplevel <- data %>%
      group_by(activity, method) %>%
      summarize(counts = n()) %>%
      ungroup()
    names(df_toplevel)[1] <- "Source Name"
    names(df_toplevel)[2] <- "Target Name"

    df_midlevel <- data %>%
      group_by(method, equipment) %>%
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
      group_by(equipment, included) %>%
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

if (interactive())
  shinyApp(ui, server)

