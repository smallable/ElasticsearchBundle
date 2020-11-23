<?php

namespace ONGR\ElasticsearchBundle\Annotation;

/**
 * @Annotation
 * @Target("PROPERTY")
 */
final class Exclude
{
    public $context;
}